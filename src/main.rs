use quick_xml::events::Event;
use quick_xml::Reader;
use serde::Deserialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::io::Write;
use std::process::Command;

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

fn request_parse(text: &str) -> Option<String> {
    // Base URL and parameters
    let base_url = "http://localhost:8080/api.php";
    let text_arg = format!("text={}", text);
    let curl_args = vec![
        "-G",
        base_url,
        "--data-urlencode",
        "action=parse",
        "--data-urlencode",
        "format=json",
        "--data-urlencode",
        "contentmodel=wikitext",
        "--data-urlencode",
        "uselang=zh-tw",
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

fn main() {
    let file = File::open("zhwiki-latest-pages-articles.xml").unwrap();
    let file = BufReader::new(file);
    let mut reader = Reader::from_reader(file);
    let mut buf = Vec::new();

    let mut inside_page = false;
    let mut inside_ns = false;
    let mut inside_id = false;
    let mut inside_text = false;
    let mut is_article = false;
    let mut article_count = 0;
    let mut current_pageid: Option<i32> = None;

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

    // Create pages/ if it doesn't exist
    std::fs::create_dir_all("pages").unwrap();

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
                    if let Ok(pageid) = e.unescape().unwrap().parse::<i32>() {
                        current_pageid = Some(pageid);
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
                        let html_text = request_parse(&text);
                        if let Some(html_text) = html_text {
                            // Output HTML to a file
                            let file_name = format!("pages/{pageid}.html");
                            let mut file = File::create(file_name).unwrap();
                            file.write_all(html_text.as_bytes()).unwrap();
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

    println!("Number of articles: {}", article_count);
    for (variant, count) in variants {
        println!("{}: {}", variant, count);
    }
}
