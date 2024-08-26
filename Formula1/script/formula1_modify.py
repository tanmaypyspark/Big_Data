import os
os.system('cls')
from utils.Utility import *

cwd = os.path.dirname(os.path.realpath(__file__))

sparka = Spark('Formula 1')
# spark = s.create



# df.show()

if __name__ =='__main__':
    
    
    # print(os.path.join(cwd,confPath))
    Conf = get_conf(os.path.join(cwd,confPath))
    
    ''' Read Raw Data '''
    rawPath = os.path.join(Conf['PATH']['base_path'],Conf['Formula1']['SourcePath'])
    circuit_path = os.path.join(rawPath, 'circuits.csv')
    # print(os.path.join(rawPath, 'circuits.csv'))
    # circuit_df = ( spark
    #               .read
    #               .format('csv')
    #               .option('header','true')
    #                .load(os.path.join(rawPath, 'circuits.csv')) )
    # circuit_df.display()

    circuit_df = sparka.read_csv(circuit_path,'true')
    # print(circuit_df)
    print('Hiiii')
    print(circuit_df.limit(1))