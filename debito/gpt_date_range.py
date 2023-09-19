import re





pattern_dias = re.compile(r'ULTIMO(S)?(\d+)DIA(S)?', re.IGNORECASE)
pattern_meses = re.compile(r'ULTIMO(S)?(\d+)MES(ES)?', re.IGNORECASE)

for input_string in ("ultimos71dias", "ultimo1dia", "ultimos12meses", "ultimo1mes"):
    if matches_dias := re.findall(pattern_dias, input_string):
        print(f"{matches_dias[0][1]=}")
    if matches_meses := re.findall(pattern_meses, input_string):
        print(f"{matches_meses[0][1]=}")



