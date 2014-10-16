import os
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
import StringIO
import json

path_read = "raw_data/pdf/"
path_save = "raw_data/json/precincts/"
month = ""
dir_year = 2014
dir_mo = 0

precincts = ["001", "030", "062", "088", "115", "005", "032", "063", "090", "120", "006", "033", "066", "094", "121", "007", "034", "067", "100", "122", "009", "040", "068", "101", "123", "010", "041", "069", "102", "city", "013", "042", "070", "103", "cot", "014", "043", "071", "104", "housing", "017", "044", "072", "105", "patrol", "018", "045", "073", "106", "pbbn", "019", "046", "075", "107", "pbbs", "020", "047", "076", "108", "pbbx", "022", "048", "077", "109", "pbmn", "023", "049", "078", "110", "pbms", "024", "050", "079", "111", "pbqn", "025", "052", "081", "112", "pbqs", "026", "060", "083", "113", "pbsi", "028", "061", "084", "114", "transit"]
precinct_data = []
tables = []


def convert_files():
        file_name = "table_pdfs/2013_stats_4.pdf"
        with open(file_name, "rb") as pdf_file:
            print(file_name)
            raw_contents = convert_file(pdf_file, file_name)
            index_locations = parse_file(raw_contents)
    #         month_data = build_monthly_data(index_locations)
    #         precinct_data["precinct_name"] = month_data.pop("precinct", "")
    #         monthly_precinct_totals.append(month_data)
    #     os.chdir("../../../")
    # write_file(monthly_precinct_totals, precinct_data, directory)


# TODO: Use csv or A for all.
def write_file(totals, precinct_data, directory):
    file_name = precinct_data["precinct"] + "_precinct.json"

    file_data = {
        "precinct": precinct_data["precinct_name"],
        "precinct_id": precinct_data["precinct"],
        "monthly_totals": totals
    }

    open_directory(directory)
    print("writing: " + path_save + file_name)
    with open(file_name, "wb") as f:
        f.write(json.dumps(file_data))

def get_footnote_index(raw_data):
    x = 0
    for line in raw_data:
        if "*" in line:
            return x
        x = x + 1
    return -1

def get_table_identifiers(raw_data):
    # Example Data: 1B: Types of Allegations in Complaints Received 2009-2013
    partitoned_data = raw_data.partition(":")
    return (partitoned_data[0].strip(), partitoned_data[2].partition("2009")[0].strip())

def get_table_footnote(raw_data):
    footnote_index = get_footnote_index(raw_data)
    if(footnote_index == -1):
        return ""
    footnote =  ""
    # footnote = footnote.join(raw_data[footnote_index:]).strip("* ")
    return "".join(raw_data[footnote_index:]).strip("* ")

def make_num(raw_val, type):
    if type == "p":
        return float(raw_val.strip("%"))
    return int(raw_val.replace(",", ""))


def get_totals(**kwargs):
    nums = [make_num(num, 'i') for num in kwargs["numbers"]]
    if kwargs["percents"] is not None and kwargs["percents"] is not "":
        percs = [make_num(num, 'p') for num in kwargs["percents"]]
    return (nums, percs)

# Tables 1A,1B
# def get_table_data(raw_data):
#     years = {}
#     year = 2009
#     number_start = 13
#     number_end = 18
#     percent_start = 22
#     percent_end = 26
#     jump_val = 15
#     rows = []
#     rows_finished = False

#     for i in range(4):
#         years[year] = {}
#         years[year]["number"], years[year]["percent_of_total"] = get_totals(numbers=raw_data[number_start:number_end], percents=raw_data[percent_start:percent_end])
#         number_start += jump_val
#         number_end += jump_val
#         percent_start += jump_val
#         percent_end += jump_val
#         year += 1

#     row_index = percent_end + 2

#     while not rows_finished:
#         rows.append(raw_data[row_index])
#         row_index += 1
#         if raw_data[row_index] == "":
#             rows_finished = True

#     return (years, rows)

def get_table_data(raw_data):
    years = {}
    year = 2009
    number_start = 13
    number_end = 18
    percent_start = 22
    percent_end = 26
    jump_val = 15
    rows = []
    rows_finished = False

    # for x in range(len(raw_data)):

    for i in range(4):
        years[year] = {}
        years[year]["number"], years[year]["percent_of_total"] = get_totals(numbers=raw_data[number_start:number_end], percents=raw_data[percent_start:percent_end])
        number_start += jump_val
        number_end += jump_val
        percent_start += jump_val
        percent_end += jump_val
        year += 1

    row_index = percent_end + 2

    while not rows_finished:
        rows.append(raw_data[row_index])
        row_index += 1
        if raw_data[row_index] == "":
            rows_finished = True

    return (years, rows)

def convert_table(str_table):
    raw_table = str_table.splitlines()
    print(raw_table)
    table = {}
    table["id"], table["name"] = get_table_identifiers(raw_table[0])
    table["footnote"] = get_table_footnote(raw_table)
    print("ID: " + table["id"] + "\nName: " + table["name"] + "\nFootnote: " + table["footnote"])
    table["data"], table["rows"] = get_table_data(raw_table)
    print("Data:")
    print(table["data"])
    print("Rows:")
    print(table["rows"])

def parse_file(raw_contents):
    print(raw_contents)
    tables = raw_contents.split("Table")
    convert_table(tables[1])

def build_monthly_data(index_locations):
    global month

    lines = index_locations["lines"]
    precinct_line = index_locations["precinct_line"]
    month_line = index_locations["month_line"]
    description_line = index_locations["description_line"]
    mtd_line = index_locations["mtd_line"]
    ytd_line = index_locations["ytd_line"]

    x = 0
    started_violations = False

    data = {}

    for line in lines:
        if x == precinct_line:
            month = str(lines[month_line])[:-1]
            data["precinct"] = line
            data["month"] = month
            data["year"] = dir_year
            data["month_no"] = dir_mo
        elif x >= description_line:
            if line == "":
                break

            if not started_violations:
                data["violations"] = []
                started_violations = True

            try:
                mtd = int(lines[(x - description_line) + mtd_line])
                ytd = int(lines[(x - description_line) + ytd_line])
            except:
                mtd = lines[(x - description_line) + mtd_line]
                ytd = lines[(x - description_line) + ytd_line]

            data["violations"].append({
                    "name": line,
                    "mtd": mtd,
                    "ytd": ytd
                })
        x += 1

    # print(data)
    return data


def convert_file(pdf_file, file_name):
    parser = PDFParser(pdf_file)
    pdf = PDFDocument(parser)
    pdf.initialize("")
    if not pdf.is_extractable:
        raise PDFPage.PDFTextExtractionNotAllowed("Document does not allow text extraction: " + file_name)

    resource = PDFResourceManager()
    # laparams = LAParams(line_overlap=0.7, char_margin=1.0, line_margin=0.5, word_margin=0.3, boxes_flow=0.15)
    laparams = LAParams()
    output = StringIO.StringIO()
    device = TextConverter(resource, output, codec="utf-8", laparams=laparams)

    interpreter = PDFPageInterpreter(resource, device)
    for page in PDFPage.create_pages(pdf):
        interpreter.process_page(page)

    return output.getvalue()


convert_files()
