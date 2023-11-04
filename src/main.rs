use itertools::Itertools;
use parquet::record::RowAccessor;
use quick_xml::events::Event;
use quick_xml::Reader;
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Write;
use std::process::Command;
use std::sync::mpsc;
use std::thread;

#[derive(Debug, Deserialize)]
struct Parse {
    title: String,
    pageid: i32,
    text: Text,
}

#[derive(Debug, Deserialize)]
struct Text {
    #[serde(rename = "*")]
    content: String,
}

#[derive(Debug, Deserialize)]
struct ParseResponse {
    parse: Parse,
}

fn main() {
    // write_to_parquet("zhwiki_hk.parquet").unwrap();

    let contents = read_from_parquet(
        "zhwiki_hk.parquet",
        HashSet::from_iter([45, 550, 672, 690, 758]),
    )
    .unwrap();
    for (id, content) in contents {
        println!("{id}");
        println!("{content}\n");
    }
}

fn request_parse(text: &str, variant: ZhVariant) -> Option<String> {
    // Base URL and parameters
    let base_url = "http://localhost:8080/api.php";
    let lang = format!("uselang={variant}");
    let text_arg = format!("text={text}");
    let curl_args = vec![
        "-X",
        "POST",
        base_url,
        "--data-urlencode",
        "action=parse",
        "--data-urlencode",
        "format=json",
        "--data-urlencode",
        "contentmodel=wikitext",
        "--data-urlencode",
        &lang,
        "--data-urlencode",
        &text_arg,
    ];

    // Use curl to make the request
    match Command::new("curl").args(&curl_args).output() {
        Ok(output) => {
            // Convert the output to a string
            let output_str = String::from_utf8(output.stdout).unwrap();

            // Parse the JSON using serde_json
            match serde_json::from_str::<ParseResponse>(&output_str) {
                Ok(res) => {
                    // Extract HTML from JSON response
                    Some(res.parse.text.content.to_string())
                }
                Err(e) => {
                    eprintln!("Failed to parse response JSON: {}", e);
                    eprintln!("{output_str}");
                    None
                }
            }
        }
        Err(err) => {
            eprintln!("Curl command failed: {err}");
            None
        }
    }
}

fn remove_tags(input: &str) -> String {
    let paired_tags = fancy_regex::Regex::new(r"<\s*(\w+)\b.*?>.*?</\s*\1\s*>").unwrap();
    let orphaned_open_tag = Regex::new(r"<\s*(\w+)\b.*?>").unwrap();
    let orphaned_close_tag = Regex::new(r"</\s*(\w+)\s*>").unwrap();
    let output = paired_tags.replace_all(input, "").to_string();
    let output = orphaned_open_tag.replace_all(&output, "").to_string();
    let output = orphaned_close_tag.replace_all(&output, "").to_string();
    output
}

fn remove_self_closing_tags(input: &str) -> String {
    let pattern = r"<\w+\b.*?/>";
    let re = Regex::new(&pattern).unwrap();
    re.replace_all(input, "").to_string()
}

// https://github.com/attardi/wikiextractor/blob/8f1b434a80608e1e313d38d263ed7c79c9ee75a9/wikiextractor/extract.py#L163
fn clean_text(text: &str) -> String {
    let mut text = text.to_string();

    // Replace '<<' with '«' and '>>' with '»'
    text = text.replace("<<", "«").replace(">>", "»");

    // Replace tabs with spaces
    text = text.replace("\t", " ");

    // Replace multiple spaces with a single space
    let re_spaces = Regex::new(r" +").unwrap();
    text = re_spaces.replace_all(&text, " ").to_string();

    // Replace multiple dots with '...'
    let re_dots = Regex::new(r"\.{3,}").unwrap();
    text = re_dots.replace_all(&text, "……").to_string();

    let re_dots = Regex::new(r"。{3,}").unwrap();
    text = re_dots.replace_all(&text, "……").to_string();

    // Handle other replacements
    let re_before = Regex::new(r" ([,，:：\.。\)）\]】»》])").unwrap();
    text = re_before.replace_all(&text, "$1").to_string();

    let re_after = Regex::new(r"([\[【\(（«《]) ").unwrap();
    text = re_after.replace_all(&text, "$1").to_string();

    // Remove lines with only punctuations
    let re_newlines = Regex::new(r"\n\W+?\n").unwrap();
    text = re_newlines.replace_all(&text, "\n").to_string();

    // Replace ',,' with ',' and ',.' with '.'
    text = text
        .replace(",,", ",")
        .replace("，，", "，")
        .replace(",.", ".")
        .replace("，。", "。");

    text
}

fn filter_lines(text: &str) -> String {
    let punctuation = Regex::new(r"\p{P}").unwrap();
    let han = Regex::new(r"\p{Han}").unwrap();

    text.lines()
        .into_iter()
        .map(|line| line.trim())
        .filter(|&line| line.chars().count() >= 10) // Filter out lines with fewer than 10 characters
        .filter(|&line| punctuation.find(line).is_some()) // Filter out lines without any punctuations
        .filter(|&line|
            // contains more than 70% of Chinese characters
            line.chars().filter(|c| han.is_match(&c.to_string())).count() as f64 > (line.chars().count() as f64) * 0.7)
        .join("\n")
}

fn html_to_text(html: &str, filter: bool) -> String {
    let mut reader = Reader::from_reader(Cursor::new(html));
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut current_is_allowed_tag = true;
    let mut current_in_p = false;
    let mut output = String::new();

    let allowed_tags: HashSet<&str> = ["b", "i", "a"].iter().cloned().collect();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name = e.name();
                let tag_name = std::str::from_utf8(name.as_ref()).unwrap();
                if tag_name == "p" {
                    current_in_p = true;
                }
                if current_in_p && allowed_tags.contains(tag_name) {
                    current_is_allowed_tag = true;
                    if tag_name == "a" {
                        for attr in e.attributes() {
                            match attr {
                                Ok(ref attribute) if attribute.key.as_ref() == b"href" => {
                                    let value = std::str::from_utf8(&attribute.value).unwrap();
                                    if value.starts_with("/index.php?title=Template:")
                                        || value.starts_with("/index.php?title=API")
                                        || value.starts_with("/index.php/File:")
                                    {
                                        // Skip this <a> tag
                                        current_is_allowed_tag = false;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let name = e.name();
                let tag_name = std::str::from_utf8(name.as_ref()).unwrap();

                if current_in_p && allowed_tags.contains(tag_name) {
                    current_is_allowed_tag = true;
                }

                if tag_name == "p" {
                    if !output.is_empty() && !output.ends_with("\n") {
                        output.push('\n');
                    }
                    current_in_p = false;
                }
            }
            Ok(Event::Text(e)) => {
                let text = e.unescape().unwrap();
                if current_in_p && current_is_allowed_tag {
                    output.push_str(&text);
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            _ => (),
        }
        buf.clear();
    }
    output = remove_tags(&output);
    output = remove_self_closing_tags(&output);

    // Remove messy parenthesized expressions
    let parenthesized_han = Regex::new(r"[（](\p{Han}+)[）]").unwrap();
    output = parenthesized_han
        .replace_all(&output, "@@start@@$1@@end@@")
        .to_string();
    let parenthesized = Regex::new(r"[（].*[）]").unwrap();
    output = parenthesized.replace_all(&output, "").to_string();
    let parenthesized_han = Regex::new(r"@@start@@(.+)@@end@@").unwrap();
    output = parenthesized_han.replace_all(&output, "（$1）").to_string();
    output = output.replace("@@start@@", "（").replace("@@end@@", "）");

    output = clean_text(&output);

    if filter {
        output = filter_lines(&output);
    }

    output.trim().to_string()
}

#[derive(Debug, Clone, Copy)]
enum ZhVariant {
    Cn,
    Hk,
    Mo,
    My,
    Sg,
    Tw,
}

impl std::fmt::Display for ZhVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ZhVariant::Cn => write!(f, "zh-cn"),
            ZhVariant::Hk => write!(f, "zh-hk"),
            ZhVariant::Mo => write!(f, "zh-mo"),
            ZhVariant::My => write!(f, "zh-my"),
            ZhVariant::Sg => write!(f, "zh-sg"),
            ZhVariant::Tw => write!(f, "zh-tw"),
        }
    }
}

fn parse_articles(variant: ZhVariant, filter: bool) {
    let file = File::open("zhwiki-latest-pages-articles.xml").unwrap();
    let file = BufReader::new(file);
    let mut reader = Reader::from_reader(file);
    let mut buf = Vec::new();

    let mut inside_page = false;
    let mut inside_ns = false;
    let mut inside_id = false;
    let mut inside_revision = true;
    let mut inside_text = false;
    let mut is_article = false;
    let mut article_count = 0;
    let mut current_pageid: Option<u64> = None;
    let mut current_revisionid: Option<u64> = None;

    let mut variants: HashMap<String, usize> = HashMap::from_iter(vec![
        ("zh-hans".to_string(), 0),
        ("zh-hant".to_string(), 0),
        ("zh-cn".to_string(), 0),
        ("zh-hk".to_string(), 0),
        ("zh-mo".to_string(), 0),
        ("zh-my".to_string(), 0),
        ("zh-sg".to_string(), 0),
        ("zh-tw".to_string(), 0),
    ]);

    // Spawn worker threads
    let mut handles = vec![];
    let mut txs = vec![];
    for _ in 0..20 {
        let (tx, rx) = mpsc::channel::<(u64, u64, String)>();
        txs.push(tx);
        let handle = thread::spawn(move || {
            loop {
                match rx.recv() {
                    Ok((pageid, revisionid, text)) => {
                        let html_text = request_parse(&text, variant);
                        if let Some(html_text) = html_text {
                            let cleaned_text = html_to_text(&html_text, filter);
                            if !cleaned_text.is_empty() {
                                let file_name = format!("pages/{pageid}_{revisionid}.txt");
                                let mut file = File::create(file_name).unwrap();
                                file.write_all(cleaned_text.as_bytes()).unwrap();
                            }
                        }
                    }
                    Err(_) => {
                        // Channel has been closed
                        break;
                    }
                }
            }
        });
        handles.push(handle);
    }

    // Create pages/ if it doesn't exist
    std::fs::create_dir_all("pages").unwrap();

    let mut current_worker = 0;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                match e.name().as_ref() {
                    b"page" => {
                        inside_page = true;
                        is_article = true; // Assume it's an article until proven otherwise
                    }
                    b"ns" => inside_ns = true,
                    b"text" => inside_text = true,
                    b"id" => {
                        if inside_page {
                            inside_id = true;
                        }
                    }
                    b"revision" => {
                        if inside_page {
                            inside_revision = true;
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Empty(ref e)) => match e.name().as_ref() {
                b"redirect" => {
                    is_article = false;
                }
                _ => {}
            },
            Ok(Event::End(ref e)) => match e.name().as_ref() {
                b"page" => {
                    inside_page = false;
                    if is_article {
                        article_count += 1;
                    }
                }
                b"text" => {
                    inside_text = false;
                }
                b"ns" => {
                    inside_ns = false;
                }
                b"id" => {
                    inside_id = false;
                }
                b"revision" => {
                    inside_revision = false;
                }
                _ => {}
            },
            Ok(Event::Text(e)) => {
                if inside_page && inside_ns {
                    let ns = e.unescape().unwrap();
                    if ns != "0" {
                        is_article = false; // It's not a main content page
                    }
                }
                if inside_page && inside_id {
                    if let Ok(id) = e.unescape().unwrap().parse::<u64>() {
                        if inside_revision {
                            current_revisionid = Some(id);
                        } else {
                            current_pageid = Some(id);
                        }
                    }
                }
                if inside_page && is_article && inside_text {
                    let text = e.unescape().unwrap();
                    for (variant, count) in &mut variants {
                        if text.contains(&format!("{variant}:")) {
                            *count += 1;
                        }
                    }
                    if let Some(pageid) = current_pageid {
                        if let Some(revisionid) = current_revisionid {
                            txs[current_worker]
                                .send((pageid, revisionid, text.to_string()))
                                .unwrap();
                            current_worker = (current_worker + 1) % 20; // Rotate workers
                        }
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                println!("Error: {:?}", e);
                break;
            }
            _ => {}
        }
        buf.clear();
    }

    // Close all channels to stop the worker threads
    for tx in txs {
        drop(tx);
    }

    // Wait for all worker threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    println!("Number of articles: {}", article_count);
    for (variant, count) in variants {
        println!("{}: {}", variant, count);
    }
}

fn get_pages_stats() {
    // Specify the path to the folder
    let path = "pages/"; // Replace this with the path to the folder you're interested in

    let mut num_files = 0;
    let mut folder_size = 0;

    // Read the directory
    match std::fs::read_dir(path) {
        Ok(entries) => {
            for entry in entries {
                match entry {
                    Ok(entry) => {
                        let path = entry.path();
                        if path.is_file() {
                            // println!("File: {}", path.display());
                            num_files += 1;
                            folder_size += entry.metadata().unwrap().len();
                        } else if path.is_dir() {
                            println!("Dir: {}", path.display());
                        }
                    }
                    Err(_) => println!("Error reading entry"),
                }
            }
        }
        Err(_) => println!("Error reading directory"),
    }

    println!("Number of files: {num_files}");
    println!("Folder size: {folder_size} bytes");
}

fn write_to_parquet(output_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    use kdam::tqdm;
    use parquet::column::writer::ColumnWriter;
    use parquet::{
        data_type::ByteArray,
        file::{properties::WriterProperties, writer::SerializedFileWriter},
        schema::parser::parse_message_type,
    };
    use std::{fs, path::Path, sync::Arc};

    let parquet_path = Path::new(output_name);

    let message_type = "
        message schema {
            REQUIRED INT64 id;
            REQUIRED INT64 revision_id;
            REQUIRED BINARY content (UTF8);
        }
    ";
    let schema = Arc::new(parse_message_type(message_type)?);

    // Writer properties to enable compression
    let props = WriterProperties::builder()
        // Change the compression type if needed, SNAPPY is a default good choice for a balance between size and speed
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let file = fs::File::create(&parquet_path)?;
    let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

    let paths = fs::read_dir("pages/")?;

    // Initialize batch vectors
    let mut ids = Vec::new();
    let mut revision_ids = Vec::new();
    let mut contents = Vec::new();
    let batch_size = 1000;

    fn write_batch(
        writer: &mut SerializedFileWriter<File>,
        ids: &[i64],
        revision_ids: &[i64],
        contents: &[ByteArray],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut row_group_writer = writer.next_row_group()?;

        // Write ID column
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(ref mut writer) = col_writer.untyped() {
                writer.write_batch(ids, None, None)?;
            }
            col_writer.close()?;
        }

        // Write Revision ID column
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            if let ColumnWriter::Int64ColumnWriter(ref mut writer) = col_writer.untyped() {
                writer.write_batch(revision_ids, None, None)?;
            }
            col_writer.close()?;
        }

        // Write Content column
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            if let ColumnWriter::ByteArrayColumnWriter(ref mut writer) = col_writer.untyped() {
                writer.write_batch(contents, None, None)?;
            }
            col_writer.close()?;
        }

        row_group_writer.close()?;
        Ok(())
    }

    for path in tqdm!(paths) {
        let path = path?.path();
        let file_name = path.file_name().unwrap().to_str().unwrap();

        if let Some((id_str, revision_id_str)) = file_name.split_once('_') {
            let id = id_str.parse::<i64>()?;
            let revision_id = revision_id_str.split('.').next().unwrap().parse::<i64>()?;
            let content = std::fs::read_to_string(&path)?;

            // Add to batch vectors
            ids.push(id);
            revision_ids.push(revision_id);
            contents.push(ByteArray::from(content.as_str()));

            // Write batch if it reaches the batch size
            if ids.len() >= batch_size {
                write_batch(&mut writer, &ids, &revision_ids, &contents)?;
                ids.clear();
                revision_ids.clear();
                contents.clear();
            }
        }
    }

    // Write any remaining items in the batch vectors
    if !ids.is_empty() {
        write_batch(&mut writer, &ids, &revision_ids, &contents)?;
    }

    writer.close()?;
    Ok(())
}

fn read_from_parquet(
    input_name: &str,
    ids: HashSet<i64>,
) -> Result<HashMap<i64, String>, Box<dyn std::error::Error>> {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::path::Path;

    // Define the path for the Parquet file.
    let parquet_path = Path::new(input_name);

    // Open the Parquet file.
    let file = File::open(&parquet_path)?;
    let reader = SerializedFileReader::new(file)?;

    // Get the Parquet file metadata.
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema();

    // Create an iterator to read row groups.
    let iter = reader.get_row_iter(Some(schema.clone()))?;

    let mut contents = HashMap::new();

    // Print the first 10 rows.
    for row in iter {
        let row = row?;
        let id: i64 = row.get_long(0).unwrap();
        if ids.contains(&id) {
            // let revision_id: i64 = row?.get_long(1).unwrap();
            let content: &str = row.get_string(2).unwrap();
            contents.insert(id, content.to_string());
        }
    }

    Ok(contents)
}
