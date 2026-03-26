import docx
doc = docx.Document(r"D:\家庭文件\侵权纠纷\起诉材料 - v03092306.docx")
try:
    sdt_deleted_count = 0
    for sdt in doc.element.body.xpath('.//w:sdt'):
        if (sdt.xpath('.//w:sdtPr//w:docPartGallery[@w:val="Table of Contents"]') or 
            sdt.xpath('.//w:instrText[contains(text(), "TOC")]') or 
            sdt.xpath('.//w:sdtPr//w:alias[contains(@w:val, "目录")]')):
            pass
except Exception as e:
    import traceback
    traceback.print_exc()
