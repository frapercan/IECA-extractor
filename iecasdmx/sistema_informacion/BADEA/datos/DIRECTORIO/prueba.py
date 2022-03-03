import pandas as pd

if __name__ == "__main__":
    one = pd.read_csv('DIRECTORIO 1/DIRECTORIO 1.csv',sep=';')
    two = pd.read_csv('DIRECTORIO 2/DIRECTORIO 2.csv',sep=';')

    print(one.columns[:4])
    # print(one.shape)
    one.drop_duplicates(subset=one.columns[:4],keep='last',inplace=True )
    print(one.shape)
    print(two.shape)
    two.drop_duplicates(subset=two.columns[:4],keep='last',inplace=True )
    print(two.shape)

    one.to_csv('DIRECTORIO 1/DIRECTORIO 1_no_dupe.csv',index=None)
    two.to_csv('DIRECTORIO 2/DIRECTORIO 2_no_dupe.csv',index=None)
    #
