# xml_to_json.py

import xml.etree.ElementTree as ET
import json

def xml_to_json(xml_file, json_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    data = {
        "country": root.attrib.get("country"),
        "source": root.attrib.get("source"),
        "lastUpdate": root.attrib.get("lastUpdate"),
        "data": []
    }

    for year in root.findall("year"):
        year_entry = {
            "year": int(year.attrib["value"]),
            "months": []
        }

        for month in year.findall("month"):
            month_name = month.attrib["name"]
            unemployment_node = month.find("unemployment")
            deaths_node = month.find("deaths")

            unemployment = {
                "national": float(unemployment_node.attrib["national"]),
                "male": float(unemployment_node.find("./gender[@sex='male']").text),
                "female": float(unemployment_node.find("./gender[@sex='female']").text)
            }

            deaths = {
                "total": int(deaths_node.attrib["total"]),
                "COVID-19": int(deaths_node.find("./cause[@type='COVID-19']").text),
                "other": int(deaths_node.find("./cause[@type='other']").text)
            }

            month_entry = {
                "name": month_name,
                "unemployment": unemployment,
                "deaths": deaths
            }

            year_entry["months"].append(month_entry)

        data["data"].append(year_entry)

    # Zapis do JSON
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ JSON zapisano w: {json_file}")
