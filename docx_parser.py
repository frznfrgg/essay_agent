import glob
import os

import docx


def docx_to_str(docx_file: str):
    doc = docx.Document(docx_file)
    full_text = "\n".join(p.text for p in doc.paragraphs)
    return full_text


def parse_docx_folder(folder_path: str):
    all_docx_texts = ""
    for filename in sorted(glob.glob(os.path.join(folder_path, "*.docx"))):
        all_docx_texts += docx_to_str(filename) + "\n\n\n"
    return all_docx_texts.strip()
