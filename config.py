from configparser import ConfigParser

def config(filename ="database.ini", section = "postgresql" ):
    #create the parser
    parser = ConfigParser()
    #read the parser
    parser.read(filename)
    
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]]  = param[1]
    else:
        raise Exception('Section {0} is not found in {1}'.format(section, filename))
    
    return db
    
    
    