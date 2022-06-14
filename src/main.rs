use std::fs::File;
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom};

use byteorder::{BigEndian, ReadBytesExt};

use crate::FilePageType::{IndexInterior, IndexLeaf, TableInterior, TableLeaf};

trait ReadVarExt: byteorder::ReadBytesExt {
    fn read_var64(&mut self) -> std::io::Result<i64> {
        let mut len = 0u64;
        let mut res = 0u64;

        loop {
            let val = self.read_u8()? as u64;

            res = (res << 7) | (val & 0x7F);
            len += 1;

            if val & 0b10000000 == 0 {
                return Ok(res as i64);
            }
        }
    }
}

impl<R: Read> ReadVarExt for R {}

#[derive(Debug)]
struct FileHeader {
    page_size: u16,
    database_size: u32,
    text_encoding: u32,
}

impl FileHeader {
    fn read<R>(reader: &mut R) -> std::io::Result<Self>
        where R: Read + Seek
    {
        reader.seek(SeekFrom::Start(16))?;
        let page_size = reader.read_u16::<BigEndian>()?;

        reader.seek(SeekFrom::Start(28))?;
        let database_size = reader.read_u32::<BigEndian>()?;

        reader.seek(SeekFrom::Start(56))?;
        let text_encoding = reader.read_u32::<BigEndian>()?;

        reader.seek(SeekFrom::Start(100))?;

        Ok(FileHeader {
            page_size,
            database_size,
            text_encoding,
        })
    }
}

#[derive(Debug)]
enum FilePageType {
    TableInterior,
    TableLeaf,
    IndexInterior,
    IndexLeaf,
}

impl FilePageType {
    fn read<R>(reader: &mut R) -> std::io::Result<Self>
        where R: Read
    {
        match reader.read_u8()? {
            0x2 => Ok(IndexInterior),
            0x5 => Ok(TableInterior),
            0xA => Ok(IndexLeaf),
            0xD => Ok(TableLeaf),
            x => Err(Error::new(ErrorKind::InvalidData, format!("Unknown file page type: {}", x))),
        }
    }
}

#[derive(Debug)]
struct FilePageHeader {
    typ: FilePageType,
    first_free_block: u16,
    cells_count: u16,
    cells_content_start: u16,
    cells_content_fragmented_bytes: u8,
    right_most_pointer: Option<u32>,
}

impl FilePageHeader {
    fn read<R>(reader: &mut R) -> std::io::Result<Self>
        where R: Read
    {
        let typ = FilePageType::read(reader)?;
        let first_free_block = reader.read_u16::<BigEndian>()?;
        let cells_count = reader.read_u16::<BigEndian>()?;
        let cells_content_start = reader.read_u16::<BigEndian>()?;
        let cells_content_fragmented_bytes = reader.read_u8()?;

        let right_most_pointer = match typ {
            TableInterior | IndexInterior => Some(reader.read_u32::<BigEndian>()?),
            _ => None
        };

        Ok(FilePageHeader {
            typ,
            first_free_block,
            cells_count,
            cells_content_start,
            cells_content_fragmented_bytes,
            right_most_pointer,
        })
    }
}

#[derive(Debug)]
struct FilePage {
    header: FilePageHeader,
    cells: Vec<FilePageCell>,
}

impl FilePage {
    fn read<R>(reader: &mut R, file_header: &FileHeader) -> std::io::Result<Self>
        where R: Read + Seek
    {
        let start = reader.stream_position()? & !(file_header.page_size as u64 - 1);
        let header = FilePageHeader::read(reader)?;

        let mut cell_offsets = Vec::new();
        let mut cells = Vec::new();

        for _ in 0..header.cells_count {
            cell_offsets.push(reader.read_u16::<BigEndian>()?);
        }

        for cell in cell_offsets.iter() {
            reader.seek(SeekFrom::Start(start + *cell as u64))?;
            cells.push(FilePageCell::read(reader, &header, file_header)?);
        }

        Ok(FilePage {
            header,
            cells,
        })
    }
}


#[derive(Debug)]
struct FilePageCell {
    payload: Option<Record>,
    left_child_page_number: Option<u32>,
    first_overflow_page_number: Option<u32>,
    rowid: Option<i64>,
}

impl FilePageCell {
    fn read<R>(reader: &mut R, page_header: &FilePageHeader, file_header: &FileHeader) -> std::io::Result<Self>
        where R: Read + Seek
    {
        let left_child_page_number = match page_header.typ {
            TableInterior | IndexInterior => Some(reader.read_u32::<BigEndian>()?),
            _ => None
        };

        let payload_length = match page_header.typ {
            TableLeaf | IndexLeaf | IndexInterior => Some(reader.read_var64()?),
            _ => None
        };

        let rowid = match page_header.typ {
            TableLeaf | TableInterior => Some(reader.read_var64()?),
            _ => None
        };

        let payload = if payload_length.is_some() {
            Some(Record::read(reader, file_header)?)
        } else {
            None
        };

        let first_overflow_page_number = match page_header.typ {
            // TableLeaf | IndexLeaf | IndexInterior => Some(reader.read_u32::<BigEndian>()?),
            _ => None
        };

        Ok(FilePageCell {
            payload,
            left_child_page_number,
            first_overflow_page_number,
            rowid,
        })
    }
}

#[derive(Debug)]
enum RecordEntry {
    Null,
    Int8(i8),
    Int16(i16),
    Int24(i32),
    Int32(i32),
    Int48(i64),
    Int64(i64),
    Float64(f64),
    Blob(Vec<u8>),
    Text(String),
}

#[derive(Debug)]
struct Record {
    entries: Vec<RecordEntry>,
}

impl Record {
    fn read<R>(reader: &mut R, file_header: &FileHeader) -> std::io::Result<Self>
        where R: Read + Seek
    {
        let record_start = reader.stream_position()?;
        let record_size = reader.read_var64()?;
        let record_end = record_start + record_size as u64;

        let mut entry_types = Vec::new();
        let mut entries = Vec::new();

        while reader.stream_position()? < record_end {
            entry_types.push(reader.read_var64()?);
        }

        for typ in entry_types.iter() {
            entries.push(match *typ {
                0 => RecordEntry::Null,
                1 => RecordEntry::Int8(reader.read_i8()?),
                2 => RecordEntry::Int16(reader.read_i16::<BigEndian>()?),
                3 => RecordEntry::Int24(reader.read_i24::<BigEndian>()?),
                4 => RecordEntry::Int32(reader.read_i32::<BigEndian>()?),
                5 => RecordEntry::Int48(reader.read_i48::<BigEndian>()?),
                6 => RecordEntry::Int64(reader.read_i64::<BigEndian>()?),
                7 => RecordEntry::Float64(reader.read_f64::<BigEndian>()?),
                8 => RecordEntry::Int8(0),
                9 => RecordEntry::Int8(1),
                x if x >= 12 && x % 2 == 0 => {
                    let mut buf = vec![0; ((x - 12) / 2) as usize];
                    reader.read_exact(&mut buf)?;
                    RecordEntry::Blob(buf)
                }
                x if x >= 13 && x % 2 == 1 => {
                    assert_eq!(file_header.text_encoding, 1);
                    let mut buf = vec![0; ((x - 13) / 2) as usize];
                    reader.read_exact(&mut buf)?;
                    RecordEntry::Text(String::from_utf8(buf).unwrap())
                }
                x => return Err(Error::new(ErrorKind::InvalidData, format!("Unknown record type: {}", x)))
            })
        }

        Ok(Record {
            entries
        })
    }
}

fn main() -> std::io::Result<()> {
    let mut file = std::env::args().nth(1)
        .map(File::open)
        .unwrap_or_else(|| Err(Error::new(ErrorKind::InvalidInput, "No input parameter specified")))?;

    let file_header = FileHeader::read(&mut file)?;
    let mut file_pages = Vec::new();

    for page_index in 1..=file_header.database_size {
        match FilePage::read(&mut file, &file_header) {
            Ok(page) => {
                println!("{}: {:#?}", page_index, page);
                file_pages.push(page);
            }
            Err(err) => println!("{}", err)
        };

        file.seek(SeekFrom::Start(file_header.page_size as u64 * page_index as u64))?;
    }

    Ok(())
}
