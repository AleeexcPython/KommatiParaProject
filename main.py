import pandas as pd

#Dataframe that reads dataset_one
path_to_file = '/Users/alexchiu/PycharmProjects/KommatiParaProject/dataset/dataset_one.csv'
client_dataset_one = pd.read_csv(path_to_file)
print(client_dataset_one)

#dataframe that reads dataset_two
path_to_file = '/Users/alexchiu/PycharmProjects/KommatiParaProject/dataset/dataset_two.csv'
client_dataset_two = pd.read_csv(path_to_file)
print(client_dataset_two)




