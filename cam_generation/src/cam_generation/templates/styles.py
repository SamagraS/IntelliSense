from docx.shared import Pt


def apply_styles(document):

    style = document.styles["Normal"]

    font = style.font
    font.name = "Calibri"
    font.size = Pt(11)

    heading = document.styles["Heading 1"]
    heading.font.name = "Calibri"
    heading.font.size = Pt(14)