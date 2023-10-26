use quick_xml::events::Event;
use quick_xml::Reader;
use std::fs::File;
use std::io::BufReader;

fn main() {
    let file = File::open("zhwiki-latest-pages-articles.xml").unwrap();
    let file = BufReader::new(file);
    let mut reader = Reader::from_reader(file);
    let mut buf = Vec::new();

    let mut inside_page = false;
    let mut inside_ns = false;
    let mut inside_text = false;
    let mut is_article = false;
    let mut article_count = 0;

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
                _ => {}
            },
            Ok(Event::Text(e)) => {
                if inside_page && inside_ns {
                    let ns = e.unescape().unwrap();
                    if ns != "0" {
                        is_article = false; // It's not a main content page
                    }
                }
                if inside_page && inside_text {
                    
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
}
