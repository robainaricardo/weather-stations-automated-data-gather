#       This python scrip get normal weather data (1981 - 2010) from INMET and insert it in a MongoDB database.

import pandas as pd 
import pymongo
import datetime
import os

# Files download URL
urlEstacoes = 'http://www.inmet.gov.br/webcdp/climatologia/normais2/imagens/normais/planilhas/1981-2010/Esta%C3%A7%C3%B5es%20Normal%20Climato%C3%B3gica%201981-2010.xls'
urlTemperatura = 'http://www.inmet.gov.br/webcdp/climatologia/normais2/imagens/normais/planilhas/1981-2010/01%20Temperatura%20M%C3%A9dia%20Compensada%20-%20Bulbo%20Seco%20NCB_1981-2010.xls'
urlPressao = 'http://www.inmet.gov.br/webcdp/climatologia/normais2/imagens/normais/planilhas/1981-2010/15%20Press%C3%A3o%20atmosf%C3%A9rica%20ao%20n%C3%ADvel%20do%20bar%C3%B4metro%20NCB_1981-2010.xls'
urlInsolacao = 'http://www.inmet.gov.br/webcdp/climatologia/normais2/imagens/normais/planilhas/1981-2010/18%20Insola%C3%A7%C3%A3o%20total%20NCB_1981-2010.xls'
urlEvaporacao = 'http://www.inmet.gov.br/webcdp/climatologia/normais2/imagens/normais/planilhas/1981-2010/19%20Evapora%C3%A7%C3%A3o%20total%20-%20Evapor%C3%ADmetro%20Piche%20NCB_1981-2010.xls'
urlNebulosidade = 'http://www.inmet.gov.br/webcdp/climatologia/normais2/imagens/normais/planilhas/1981-2010/20%20Nebulosidade%20NCB_1981-2010.xls'
urlUmidade = 'http://www.inmet.gov.br/webcdp/climatologia/normais2/imagens/normais/planilhas/1981-2010/24%20Umidade%20Relativa%20do%20ar%20NCB_1981-2010.xls'
urlPrecipitacao = 'http://www.inmet.gov.br/webcdp/climatologia/normais2/imagens/normais/planilhas/1981-2010/30%20Precipita%C3%A7%C3%A3o%20Acumulada%20NCB_1981-2010.xls'
urlVento = 'http://www.inmet.gov.br/webcdp/climatologia/normais2/imagens/normais/planilhas/1981-2010/44%20Intensidade%20do%20Vento%20NCB_1981-2010.xls'
urlEvapotranspiracao = 'http://www.inmet.gov.br/webcdp/climatologia/normais2/imagens/normais/planilhas/1981-2010/49%20Evapotranspira%C3%A7%C3%A3o%20Potencial%20NCB_1981-2010.xls'

# Name of the downloaded files
fileEstacoes = 'Estações Normal Climatoógica 1981-2010.xls'
fileTemperatura = '01 Temperatura Média Compensada - Bulbo Seco NCB_1981-2010.xls'
filePressao = '15 Pressão atmosférica ao nível do barômetro NCB_1981-2010.xls'
fileInsolacao = '18 Insolação total NCB_1981-2010.xls'
fileEvaporacao = '19 Evaporação total - Evaporímetro Piche NCB_1981-2010.xls'
fileNebulosidade = '20 Nebulosidade NCB_1981-2010.xls'
fileUmidade = '24 Umidade Relativa do ar NCB_1981-2010.xls'
filePrecipitacao = '30 Precipitação Acumulada NCB_1981-2010.xls'
fileVento = '44 Intensidade do Vento NCB_1981-2010.xls'
fileEvapotranspiracao = '49 Evapotranspiração Potencial NCB_1981-2010.xls'

# Start a MongoDB database connection
print('Starting the database connection...')
client = pymongo.MongoClient('mongodb://ric:senha@ppgcap-shard-00-00-trdxc.mongodb.net:27017,ppgcap-shard-00-01-trdxc.mongodb.net:27017,ppgcap-shard-00-02-trdxc.mongodb.net:27017/test?ssl=true&replicaSet=PPGCAP-shard-0&authSource=admin&retryWrites=true')
client = pymongo.MongoClient('localhost', 27017)
db = client
print('The database connection have been started successfully!')

# Main function
def main():
    #   List with the data to be downloaded
    dataList = [
        urlEstacoes,
        urlTemperatura,
        urlPressao,
        urlInsolacao,
        urlEvaporacao,
        urlNebulosidade,
        urlUmidade,
        urlPrecipitacao,
        urlVento,
        urlEvapotranspiracao
    ]

    #   List of the downloaded files
    filesList = [
        fileEstacoes,
        fileTemperatura,
        filePressao,
        fileInsolacao,
        fileEvaporacao,
        fileNebulosidade,
        fileUmidade,
        filePrecipitacao,
        fileVento,
        fileEvapotranspiracao
    ]
    #   Download the data
    downloadData(dataList)
    #   Insert the data trough the urlEstacoes file
    insertStations(fileEstacoes)
    #   Update the database with other informations
    updateData(fileTemperatura, 'temperatura')
    updateData(filePressao, 'pressao')
    updateData(fileInsolacao, 'insolacao')
    updateData(fileEvaporacao, 'evaporacao')
    updateData(fileNebulosidade, 'nebulosidade')
    updateData(fileUmidade, 'umidade')
    updateData(filePrecipitacao, 'precipitacao')
    updateData(fileVento, 'vento')
    updateData(fileEvapotranspiracao, 'evapotranspiracao')
    #   Delete the files downloaded
    deleteFiles(filesList)

#       This function download all the files in a list of URLs
def downloadData(lista):
    print('Fazendo download dos arquivos.')
    for e in lista:
        os.system("wget " + e)
    os.system("ls -l")

#       This function insert in a MongoDB database all the weather stations from INMET (fileEstacoes) 
def insertStations(fileName):
    file = fileName
    dados =  pd.ExcelFile(file)
    estacoes = dados.parse(dados.sheet_names[0], skiprows=4)
    for x in estacoes.get_values():
        print(x)
        db.clima.estacoes.insert_one({'numero': x[0], 'codigo': x[1], 'nome': x[2], 'UF': x[3], 'localizacao': {'coordinates': [ float(x[5]), float(x[4])], "type": "Point"}, 'altitude': x[6], 'inicio_operacao': x[7], 'fim_operacao': x[8], 'situacao': x[9]})  # Insere no banco de dados
        
#       This function update (append) in a MongoDB database other data from INMET files 
#       fileName is the file from INMET
#       updateField is the name of the new filed
def updateData(fileName, updateField):
    file = fileName
    dados =  pd.ExcelFile(file)
    estacoes = dados.parse(dados.sheet_names[0], skiprows=4)
    for x in estacoes.get_values():
        print(x)
        db.clima.estacoes.update({'codigo': x[0]}, {'$set': {updateField: [{'janeiro': x[3]}, {'fevereiro': x[4]}, {'marco': x[5]}, {'abril': x[6]}, {'maio': x[7]}, {'junho': x[8]}, {'julho': x[9]}, {'agosto': x[10]}, {'setembro': x[11]}, {'outubro': x[12]}, {'novembro': x[13]}, {'dezembro': x[14]}, {'ano': x[15]}]}})

#       This funcition delete all downloaded files 
def deleteFiles(filesList):
    for e in filesList:
        os.system("rm -R " + '"' + e + '"')

if __name__ == "__main__":
    main()