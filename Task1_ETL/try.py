import xml.etree.ElementTree as ET

try:
    tree = ET.parse('your_file.xml')
    root = tree.getroot()

    for person in root.findall('person'):
        name = person.find('name').text
        height = float(person.find('height').text)
        weight = float(person.find('weight').text)

        print(f"Name: {name}, Height: {height}, Weight: {weight}")

except ET.ParseError as e:
    print(f"Error parsing XML: {e}")
