from pymongo import MongoClient
import pymongo
import csv
import pandas as pd
import datetime

first_time = datetime.datetime.now()
client = MongoClient()
db = client['SeriesDB']
series_collection = db['series']

files = ["Odata2019File.csv", "Odata2020File.csv"]
years = [2019, 2020]

def proverka_na_chislo(x):
    if x == 'null':
        return 0
    else:
        return float(x.replace(',', '.'))

def insert_document(collection, data):
    return collection.insert_one(data)

def find_document(collection, elements, multiple=False):
    if multiple:
        results = collection.find(elements)
        return [r for r in results]
    else:
        return collection.find_one(elements)

num = 0
k=0
while num != 2:
    with open(files[num]) as r_file:
        file_reader = csv.reader(r_file, delimiter=';')
        try:
            for row in file_reader:
                if k > 0:
                    OUTID = row[0]
                    Birth = row[1]
                    SEXTYPENAME = row[2]
                    REGNAME = row[3]
                    AREANAME = row[4]
                    TERNAME = row[5]
                    REGTYPENAME = row[6]
                    TerTypeName = row[7]
                    ClassProfileNAME = row[8]
                    ClassLangName= row[9]
                    EONAME = row[10]
                    EOTYPENAME = row[11]
                    EORegName = row[12]
                    EOAreaName = row[13]
                    EOTerName = row[14]
                    EOParent = row[15]
                    UkrTest= row[16]
                    UkrTestStatus = row[17]
                    UkrBall100 = proverka_na_chislo(row[18])
                    UkrBall12 = proverka_na_chislo (row[19])
                    UkrBall= proverka_na_chislo (row[20])
                    UkrAdaptScale = row[21]
                    UkrPTName = row[22]
                    UkrPTRegName= row[23]
                    UkrPTAreaName = row[24]
                    UkrPTTerName= row[25]
                    histTest= row[26]
                    HistLang = row[27]
                    histTestStatus = row[28]
                    histBall100 =  proverka_na_chislo (row[29])
                    histBall12 =  proverka_na_chislo (row[30])
                    histBall= proverka_na_chislo (row[31])
                    histPTName = row[32]
                    histPTRegName= row[33]
                    histPTAreaName = row[34]
                    histPTTerName= row[35]
                    mathTest = row[36]
                    mathLang = row[37]
                    mathTestStatus = row[38]
                    mathBall100 = proverka_na_chislo(row[39])
                    mathBall12 = proverka_na_chislo(row[40])
                    mathBall = proverka_na_chislo(row[41])
                    mathPTName = row[42]
                    mathPTRegName = row[43]
                    mathPTAreaName = row[44]
                    mathPTTerName = row[45]
                    physTest = row[46]
                    physLang = row[47]
                    physTestStatus= row[48]
                    physBall100 = proverka_na_chislo(row[49])
                    physBall12 = proverka_na_chislo(row[50])
                    physBall = proverka_na_chislo(row[51])
                    physPTName = row[52]
                    physPTRegName = row[53]
                    physPTAreaName = row[54]
                    physPTTerName = row[55]
                    chemTest = row[56]
                    chemLang = row[57]
                    chemTestStatus = row[58]
                    chemBall100 = proverka_na_chislo(row[59])
                    chemBall12 = proverka_na_chislo(row[60])
                    chemBall = proverka_na_chislo(row[61])
                    chemPTName = row[62]
                    chemPTRegName = row[63]
                    chemPTAreaName = row[64]
                    chemPTTerName = row[65]
                    bioTest= row[66]
                    bioLang= row[67]
                    bioTestStatus = row[68]
                    bioBall100 = proverka_na_chislo(row[69])
                    bioBall12 = proverka_na_chislo(row[70])
                    bioBall = proverka_na_chislo(row[71])
                    bioPTName = row[72]
                    bioPTRegName= row[73]
                    bioPTAreaName = row[74]
                    bioPTTerName= row[75]
                    geoTest= row[76]
                    geoLang = row[77]
                    geoTestStatus = row[78]
                    geoBall100 = proverka_na_chislo(row[79])
                    geoBall12 = proverka_na_chislo(row[80])
                    geoBall = proverka_na_chislo(row[81])
                    geoPTName = row[82]
                    geoPTRegName = row[83]
                    geoPTAreaName = row[84]
                    geoPTTerName= row[85]
                    engTest= row[86]
                    engTestStatus = row[87]
                    engBall100 = proverka_na_chislo(row[88])
                    engBall12 = proverka_na_chislo(row[89])
                    engDPALevel = row[90]
                    engBall= proverka_na_chislo(row[91])
                    engPTName = row[92]
                    engPTRegName = row[93]
                    engPTAreaName = row[94]
                    engPTTerName = row[95]
                    fraTest= row[96]
                    fraTestStatus = row[97]
                    fraBall100 = proverka_na_chislo(row[98])
                    fraBall12 = proverka_na_chislo(row[99])
                    fraDPALevel = row[100]
                    fraBall = proverka_na_chislo(row[101])
                    fraPTName = row[102]
                    fraPTRegName= row[103]
                    fraPTAreaName = row[104]
                    fraPTTerName = row[105]
                    deuTest = row[106]
                    deuTestStatus = row[107]
                    deuBall100 =  proverka_na_chislo(row[108])
                    deuBall12 = proverka_na_chislo(row[109])
                    deuDPALevel = row[110]
                    deuBall= proverka_na_chislo(row[111])
                    deuPTName = row[112]
                    deuPTRegName = row[113]
                    deuPTAreaName = row[114]
                    deuPTTerName= row[115]
                    spaTest= row[116]
                    spaTestStatus = row[117]
                    spaBall100 = proverka_na_chislo(row[118])
                    spaBall12 = proverka_na_chislo(row[119])
                    spaDPALevel = row[120]
                    spaBall = proverka_na_chislo(row[121])
                    spaPTName = row[122]
                    spaPTRegName= row[123]
                    spaPTAreaName = row[124]
                    spaPTTerName = row[125]
                    year = years[num]
                    new_doc = {'OUTID': OUTID, 'Birth': Birth, 'SEXTYPENAME': SEXTYPENAME, 'REGNAME': REGNAME,  'AREANAME': AREANAME,  'TERNAME': TERNAME, 'REGTYPENAME': REGTYPENAME,
                    'TerTypeName': TerTypeName, 'ClassProfileNAME': ClassProfileNAME, 'ClassLangName': ClassLangName, 'EONAME': EONAME, 'EOTYPENAME': EOTYPENAME, 'EORegName': EORegName,
                    'EOAreaName': EOAreaName, 'EOTerName': EOTerName, 'EOParent': EOParent, 'UkrTest': UkrTest, 'UkrTestStatus': UkrTestStatus, 'UkrBall100': UkrBall100, 'UkrBall12': UkrBall12,
                    'UkrBall': UkrBall, 'UkrAdaptScale': UkrAdaptScale, 'UkrPTName': UkrPTName, 'UkrPTRegName': UkrPTRegName, 'UkrPTAreaName': UkrPTAreaName, 'UkrPTTerName': UkrPTTerName,
                    'histTest': histTest, 'HistLang': HistLang, 'histTestStatus': histTestStatus, 'histBall100': histBall100, 'histBall12': histBall12, 'histBall': histBall, 'histPTName': histPTName,
                    'histPTRegName': histPTRegName, 'histPTAreaName': histPTAreaName, 'histPTTerName': histPTTerName, 'mathTest': mathTest, 'mathLang': mathLang, 'mathTestStatus': mathTestStatus,
                    'mathBall100': mathBall100, 'mathBall12': mathBall12, 'mathBall': mathBall, 'mathPTName': mathPTName, 'mathPTRegName': mathPTRegName, 'mathPTAreaName': mathPTAreaName,
                    'mathPTTerName': mathPTTerName, 'physTest': physTest, 'physLang': physLang, 'physTestStatus': physTestStatus, 'physBall100': physBall100, 'physBall12': physBall12,
                    'physBall': physBall, 'physPTName': physPTName, 'physPTRegName': physPTRegName, 'physPTAreaName': physPTAreaName, 'physPTTerName': physPTTerName, 'chemTest': chemTest,
                    'chemLang': chemLang, 'chemTestStatus': chemTestStatus, 'chemBall100': chemBall100, 'chemBall12': chemBall12, 'chemBall': chemBall, 'chemPTName': chemPTName,
                    'chemPTRegName': chemPTRegName, 'chemPTAreaName': chemPTAreaName, 'chemPTTerName': chemPTTerName, 'bioTest': bioTest, 'bioLang': bioLang, 'bioTestStatus': bioTestStatus,
                    'bioBall100': bioBall100, 'bioBall12': bioBall12, 'bioBall': bioBall, 'bioPTName': bioPTName, 'bioPTRegName': bioPTRegName, 'bioPTAreaName': bioPTAreaName,
                    'bioPTTerName': bioPTTerName, 'geoTest': geoTest, 'geoLang': geoLang, 'geoTestStatus': geoTestStatus, 'geoBall100': geoBall100, 'geoBall12': geoBall12, 'geoBall': geoBall,
                    'geoPTName': geoPTName, 'geoPTRegName': geoPTRegName, 'geoPTAreaName': geoPTAreaName, 'geoPTTerName': geoPTTerName, 'engTest': engTest, 'engTestStatus': engTestStatus,
                    'engBall100': engBall100, 'engBall12': engBall12, 'engDPALevel': engDPALevel, 'engBall': engBall, 'engPTName': engPTName, 'engPTRegName': engPTRegName, 'engPTAreaName': engPTAreaName,
                    'engPTTerName': engPTTerName, 'fraTest': fraTest, 'fraTestStatus': fraTestStatus, 'fraBall100': fraBall100, 'fraBall12': fraBall12, 'fraDPALevel': fraDPALevel,
                    'fraBall': fraBall, 'fraPTName': fraPTName, 'fraPTRegName': fraPTRegName, 'fraPTAreaName': fraPTAreaName, 'fraPTTerName': fraPTTerName, 'deuTest': deuTest,
                    'deuTestStatus': deuTestStatus, 'deuBall100': deuBall100, 'deuBall12': deuBall12, 'deuDPALevel': deuDPALevel, 'deuBall': deuBall, 'deuPTName': deuPTName,
                    'deuPTRegName': deuPTRegName, 'deuPTAreaName': deuPTAreaName, 'deuPTTerName': deuPTTerName, 'spaTest': spaTest, 'spaTestStatus': spaTestStatus, 'spaBall100': spaBall100,
                    'spaBall12': spaBall12, 'spaDPALevel': spaDPALevel, 'spaBall': spaBall, 'spaPTName': spaPTName, 'spaPTRegName': spaPTRegName, 'spaPTAreaName': spaPTAreaName,
                    'spaPTTerName': spaPTTerName, 'Year': year}
                    insert_document(series_collection, new_doc)
                k = k+1
                print(k)

        except:
            print(f'Error on the line ', k-1)
            raise
        finally:
            r_file.close()
            # con.commit()
    num = num + 1
    k = 0

later_time = datetime.datetime.now()
difference = later_time - first_time
print('Час загрузки - ', difference)

result = series_collection.aggregate([{'$match': {'Year': 2019}}, {'$group': {'_id' : '$REGNAME', 'avg_bal' : {'$avg' : '$UkrBall100'}}}])
result1 = series_collection.aggregate([{'$match': {'Year': 2020}}, {'$group': {'_id' : '$REGNAME', 'avg_bal' : {'$avg' : '$UkrBall100'}}}])
result_all = {}
for doc in result:
    result_all[doc['_id']] = {2019: doc['avg_bal']}
for doc in result1:
    result_all[doc['_id']].update({2020: doc['avg_bal']})
result_all = pd.DataFrame(result_all).T

result_all.to_csv('result_all.csv', sep='\t')
