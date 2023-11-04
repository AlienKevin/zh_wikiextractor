use core::num;
use itertools::Itertools;
use kdam::{tqdm, BarExt};
use parquet::column::writer::ColumnWriter;
use parquet::record::RowAccessor;
use parquet::{
    data_type::ByteArray,
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use quick_xml::events::Event;
use quick_xml::Reader;
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::io::Cursor;
use std::process::Command;
use std::sync::{mpsc, Mutex};
use std::thread::{self};
use std::{fs, path::Path, sync::Arc};

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
    // let xml_filename = "zhwiki-latest-pages-articles.xml";
    let xml_filename = "zhwiki-short.xml";
    parse_articles(xml_filename, ZhVariant::Tw, false).unwrap();

    let pages = read_from_parquet(
        "wikipedia-zh-tw.parquet",
        HashSet::from_iter([45, 550, 672, 690, 758]),
    )
    .unwrap();
    for page in pages {
        println!("{page:#?}");
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
        // "--data-urlencode",
        // "section=new",
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

    // Remove section headers
    let section_header = Regex::new(r"==.+==\s*\n?").unwrap();
    output = section_header.replace_all(&output, "").to_string();

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

#[derive(Debug, Clone)]
struct Page {
    page_id: i64,
    revision_id: i64,
    timestamp: i64,
    title: String,
    content: String,
}

fn count_pages(xml_filename: &str) -> quick_xml::Result<usize> {
    let file = File::open(xml_filename)?;
    let file = BufReader::new(file);
    let mut reader = Reader::from_reader(file);

    let mut count = 0;
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) if e.name().as_ref() == b"page" => {
                count += 1;
            }
            Ok(Event::Eof) => break, // Exit the loop when reaching end of file
            Err(e) => return Err(e),
            _ => (), // There are several other Event variants that we do not handle here
        }
        buf.clear();
    }
    Ok(count)
}

fn parse_articles(
    xml_filename: &str,
    variant: ZhVariant,
    filter: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Set up Parquet writer
    let output_name = format!("wikipedia-{}.parquet", variant);
    let parquet_path = Path::new(output_name.as_str());

    let message_type = "
        message schema {
            REQUIRED INT64 id;
            REQUIRED INT64 revision_id;
            REQUIRED INT64 timestamp (TIMESTAMP_MILLIS);
            REQUIRED BINARY title (UTF8);
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
    let writer = Arc::new(Mutex::new(SerializedFileWriter::new(
        file,
        schema,
        Arc::new(props),
    )?));

    // Initialize batch vectors
    let pages = Arc::new(Mutex::new(vec![]));
    let batch_size = 1000;

    let num_pages = count_pages(xml_filename)?;
    // Initialize progress bar
    let progress_bar = Arc::new(Mutex::new(tqdm!(total = num_pages)));

    // Read XML file
    let file = File::open(xml_filename).unwrap();
    let file = BufReader::new(file);
    let mut reader = Reader::from_reader(file);
    let mut buf = Vec::new();

    let mut inside_page = false;
    let mut inside_ns = false;
    let mut inside_id = false;
    let mut inside_revision = false;
    let mut inside_title = false;
    let mut inside_text = false;
    let mut inside_timestamp = false;

    let mut is_article = false;
    let mut article_count = 0;
    let mut current_pageid: Option<i64> = None;
    let mut current_revisionid: Option<i64> = None;
    let mut current_timestamp: Option<i64> = None;
    let mut current_title: Option<String> = None;

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
        let (tx, rx) = mpsc::channel::<Page>();
        txs.push(tx);
        let pages = pages.clone();
        let writer = writer.clone();
        let progress_bar = progress_bar.clone();
        let handle = thread::spawn(move || {
            loop {
                match rx.recv() {
                    Ok(Page {
                        page_id: pageid,
                        revision_id: revisionid,
                        timestamp,
                        title,
                        content: text,
                    }) => {
                        let html_title = request_parse(&title, variant);
                        if let Some(html_title) = html_title {
                            let title = html_to_text(&html_title, false);
                            if !title.is_empty() {
                                let html_text = request_parse(&text, variant);
                                if let Some(html_text) = html_text {
                                    println!("{html_text}");
                                    let cleaned_text = html_to_text(&html_text, filter);
                                    if !cleaned_text.is_empty() {
                                        // Add to batch vectors
                                        let mut pages = pages.lock().unwrap();
                                        pages.push(Page {
                                            page_id: pageid,
                                            revision_id: revisionid,
                                            timestamp,
                                            title,
                                            content: cleaned_text,
                                        });
                                        progress_bar.lock().unwrap().update(1).unwrap();

                                        // Write batch if it reaches the batch size
                                        if pages.len() >= batch_size {
                                            let mut writer = writer.lock().unwrap();
                                            write_batch(&mut writer, &pages).unwrap();
                                            pages.clear();
                                        }
                                    }
                                }
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
                    b"id" => inside_id = true,
                    b"revision" => inside_revision = true,
                    b"title" => inside_title = true,
                    b"timestamp" => inside_timestamp = true,
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
                b"timestamp" => {
                    inside_timestamp = false;
                }
                b"title" => {
                    inside_title = false;
                }
                _ => {}
            },
            Ok(Event::Text(e)) => {
                if inside_page {
                    if inside_ns {
                        let ns = e.unescape().unwrap();
                        if ns != "0" {
                            is_article = false; // It's not a main content page
                        }
                    } else if inside_id {
                        if let Ok(id) = e.unescape().unwrap().parse::<i64>() {
                            if inside_revision {
                                current_revisionid = Some(id);
                            } else {
                                current_pageid = Some(id);
                            }
                        }
                    } else if inside_revision && inside_timestamp {
                        let timestamp_str = e.unescape().unwrap();
                        let timestamp = chrono::NaiveDateTime::parse_from_str(&timestamp_str, "%+")
                            .map_err(|e| format!("Failed to parse timestamp: {}", e))?;
                        let timestamp_millis = timestamp.timestamp_millis();
                        current_timestamp = Some(timestamp_millis);
                    } else if inside_title {
                        current_title = Some(e.unescape().unwrap().to_string());
                    } else if is_article && inside_text {
                        let text = e.unescape().unwrap();
                        for (variant, count) in &mut variants {
                            if text.contains(&format!("{variant}:")) {
                                *count += 1;
                            }
                        }
                        if let Some(pageid) = current_pageid {
                            if let Some(revisionid) = current_revisionid {
                                if let Some(timestamp) = current_timestamp.as_ref() {
                                    if let Some(title) = current_title.as_ref() {
                                        txs[current_worker]
                                            .send(Page {
                                                page_id: pageid,
                                                revision_id: revisionid,
                                                timestamp: *timestamp,
                                                title: title.to_string(),
                                                content: text.to_string(),
                                            })
                                            .unwrap();
                                        current_worker = (current_worker + 1) % 20;
                                        // Rotate workers
                                    }
                                }
                            }
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

    // Write any remaining items in the batch vectors
    let pages = pages.lock().unwrap();
    if !pages.is_empty() {
        write_batch(&mut writer.lock().unwrap(), &pages)?;
    }

    Arc::try_unwrap(writer)
        .unwrap()
        .into_inner()
        .unwrap()
        .close()?;

    println!("Number of articles: {}", article_count);
    for (variant, count) in variants {
        println!("{}: {}", variant, count);
    }

    Ok(())
}

fn write_batch(
    writer: &mut SerializedFileWriter<File>,
    pages: &[Page],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut row_group_writer = writer.next_row_group()?;

    // Write ID column
    if let Some(mut col_writer) = row_group_writer.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(ref mut writer) = col_writer.untyped() {
            writer.write_batch(
                &pages
                    .iter()
                    .map(|Page { page_id, .. }| *page_id)
                    .collect::<Vec<_>>(),
                None,
                None,
            )?;
        }
        col_writer.close()?;
    }

    // Write Revision ID column
    if let Some(mut col_writer) = row_group_writer.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(ref mut writer) = col_writer.untyped() {
            writer.write_batch(
                &pages
                    .iter()
                    .map(|Page { revision_id, .. }| *revision_id)
                    .collect::<Vec<_>>(),
                None,
                None,
            )?;
        }
        col_writer.close()?;
    }

    // Write Timestamp column
    if let Some(mut col_writer) = row_group_writer.next_column()? {
        if let ColumnWriter::Int64ColumnWriter(ref mut writer) = col_writer.untyped() {
            writer.write_batch(
                &pages
                    .iter()
                    .map(|Page { timestamp, .. }| *timestamp)
                    .collect::<Vec<_>>(),
                None,
                None,
            )?;
        }
        col_writer.close()?;
    }

    // Write Title column
    if let Some(mut col_writer) = row_group_writer.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(ref mut writer) = col_writer.untyped() {
            writer.write_batch(
                &pages
                    .iter()
                    .map(|Page { title, .. }| ByteArray::from(title.as_str()))
                    .collect::<Vec<_>>(),
                None,
                None,
            )?;
        }
        col_writer.close()?;
    }

    // Write Content column
    if let Some(mut col_writer) = row_group_writer.next_column()? {
        if let ColumnWriter::ByteArrayColumnWriter(ref mut writer) = col_writer.untyped() {
            writer.write_batch(
                &pages
                    .iter()
                    .map(|Page { content, .. }| ByteArray::from(content.as_str()))
                    .collect::<Vec<_>>(),
                None,
                None,
            )?;
        }
        col_writer.close()?;
    }

    row_group_writer.close()?;
    Ok(())
}

fn read_from_parquet(
    input_name: &str,
    ids: HashSet<i64>,
) -> Result<Vec<Page>, Box<dyn std::error::Error>> {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

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

    let mut pages = vec![];

    // Print the first 10 rows.
    for row in iter {
        let row = row?;
        let page_id: i64 = row.get_long(0).unwrap();
        if ids.contains(&page_id) {
            let revision_id: i64 = row.get_long(1).unwrap();
            let timestamp_millis: i64 = row.get_timestamp_millis(2).unwrap();
            let title: &str = row.get_string(3).unwrap();
            let content: &str = row.get_string(4).unwrap();
            pages.push(Page {
                page_id,
                revision_id,
                timestamp: timestamp_millis,
                title: title.to_string(),
                content: content.to_string(),
            });
        }
    }

    Ok(pages)
}
