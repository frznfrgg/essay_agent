import docx
import os
import glob
def docx_to_str(docx_file: str):
    doc = docx.Document(docx_file)
    doc_name = os.path.basename(docx_file)[:-5]  # Remove .docx extension
    full_text = "\n".join(p.text for p in doc.paragraphs)
    #formatted_text = f"Document {doc_name}.\n{full_text}"
    return full_text

def parse_docx_folder(folder_path: str):
    all_docx_texts = ""
    for filename in sorted(glob.glob(os.path.join(folder_path, "*.docx"))):
        all_docx_texts += docx_to_str(filename) + "\n\n\n"
    return all_docx_texts.strip()
