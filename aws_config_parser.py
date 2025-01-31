import configparser

configPorfile = configparser.ConfigParser()
configPorfile.sections()
configPorfile.read('/root/.aws/config')
lstConfigProfile = configPorfile.sections()
lstConfig = []
for config in lstConfigProfile :
  try : 
    if len(config.split()) > 1 :
      lstConfig.append(config.split()[1])
    else :
      lstConfig.append(config)
  except Exception as e : 
    print(f"config parser exception : {config}\n{e}"
