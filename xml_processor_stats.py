import xml.etree.ElementTree as ET
import os

def import_extended_xml(filepath):
    tree = ET.parse(filepath)
    root = tree.getroot()

    all_data = []

    for year_elem in root.findall('year'):
        year_value = int(year_elem.attrib['value'])
        for month_elem in year_elem.findall('month'):
            month_name = month_elem.attrib['name']

            # Unemployment
            unemployment_elem = month_elem.find('unemployment')
            unemployment_national = float(unemployment_elem.attrib['national'])
            gender_data = {
                'male': None,
                'female': None
            }
            for gender_elem in unemployment_elem.findall('gender'):
                gender_data[gender_elem.attrib['sex']] = float(gender_elem.text)

            # Deaths
            deaths_elem = month_elem.find('deaths')
            deaths_total = int(deaths_elem.attrib['total'])
            covid = 0
            other = 0
            for cause_elem in deaths_elem.findall('cause'):
                if cause_elem.attrib['type'] == "COVID-19":
                    covid = int(cause_elem.text)
                elif cause_elem.attrib['type'] == "other":
                    other = int(cause_elem.text)

            # Додаємо все до списку
            all_data.append({
                'year': year_value,
                'month': month_name,
                'unemployment': unemployment_national,
                'male': gender_data['male'],
                'female': gender_data['female'],
                'deaths_total': deaths_total,
                'deaths_covid': covid,
                'deaths_other': other
            })

    return all_data


def export_extended_xml(data, output_filepath):
    root = ET.Element('statistics')

    year_groups = {}
    for item in data:
        y = item['year']
        if y not in year_groups:
            year_groups[y] = []
        year_groups[y].append(item)

    for year, items in year_groups.items():
        year_elem = ET.SubElement(root, 'year', {'value': str(year)})
        for entry in items:
            month_elem = ET.SubElement(year_elem, 'month', {'name': entry['month']})

            unemployment_elem = ET.SubElement(month_elem, 'unemployment', {
                'national': str(entry['unemployment'])
            })
            ET.SubElement(unemployment_elem, 'gender', {'sex': 'male'}).text = str(entry['male'])
            ET.SubElement(unemployment_elem, 'gender', {'sex': 'female'}).text = str(entry['female'])

            deaths_elem = ET.SubElement(month_elem, 'deaths', {
                'total': str(entry['deaths_total'])
            })
            ET.SubElement(deaths_elem, 'cause', {'type': 'COVID-19'}).text = str(entry['deaths_covid'])
            ET.SubElement(deaths_elem, 'cause', {'type': 'other'}).text = str(entry['deaths_other'])

    tree = ET.ElementTree(root)
    tree.write(output_filepath, encoding='utf-8', xml_declaration=True)


if __name__ == "__main__":
    input_path = os.path.join('data', 'raw', 'covid_stats.xml')
    output_path = os.path.join('data', 'processed', 'covid_stats_exported.xml')

    print("📥 Import z XML...")
    stats = import_extended_xml(input_path)
    for record in stats:
        print(record)

    print("\n📤 Export w XML...")
    export_extended_xml(stats, output_path)
    print(f"✅ Dane zostały zapisane w: {output_path}")
