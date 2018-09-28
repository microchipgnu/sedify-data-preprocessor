import numpy as np
import json
import sys
import h5py
import os
import array
from collections import defaultdict


def multi_level_dict():
    return defaultdict(multi_level_dict)

def visitor(name, node):
    if isinstance(node, h5py.Dataset):
        dataset = node.name.split('/')
        names_visited.append(dataset[-1])
        #print(dataset[1:])

aux_dir = "1.hdf5"
files_folder = 'datasets/'
files = []

names_visited = []
to_visit_groups = ["sediment", "temperature", "salinity", "Latitude", "Longitude", "Z Pos"]
timestep = 1
aux_timestep = 1

for elm in sys.argv[1:]:
    files.append(str(elm))

if not os.path.exists('output'):
    os.makedirs('output')

dir = os.path.join('output', '1.hdf5')
if not os.path.exists(dir):
    os.makedirs(dir)

for file in files:
    print("ENTERING: " + file)

    #getting last timestep to know where to start counting. to have the right sequence
    timestep = aux_timestep

    """Open file(s)"""
    try:
        hf = h5py.File(files_folder + file, 'r')
    except:
        print('Check if you have file <' + file + '> on your ' + files_folder)
        break

    """Get data from Grid """
    data = hf["Grid"]
    att = data.keys()

    #our arrays - Bathymetry, Longitude and Latitude
    att_data_bathymetry = data["Bathymetry"][:]
    att_data_latitude = data["Latitude"][:]
    att_data_longitude = data["Longitude"][:]


    att_data_bathymetry.astype('float32').tofile(dir + '/bathymetry.bin')
    att_data_longitude.astype('float32').tofile(dir + '/longitude.bin')
    att_data_latitude.astype('float32').tofile(dir + '/latitude.bin')

    #my_dict = multi_level_dict()

    """Get data from Time """

    data = hf["Time"]
    #current_dir = aux_dir + "/Time/"
    current_dir = aux_dir + "/Time/"

    for subgroup in data:

        """
        Time: Year, Month, Day, Hour, ...
        This is a 6 dimensional array, but only 4 dimensions were considered (until Hour) for the sake of memory.
        """

        visited_dir = os.path.join('output', current_dir + "/")
        if not os.path.exists(visited_dir):
            os.makedirs(visited_dir)

        # my_dict[count]['Time']['Year'] = str(time_subgroup[0])
        # my_dict[count]['Time']['Month'] = str(time_subgroup[1])
        # my_dict[count]['Time']['Day'] = str(time_subgroup[2])
        # my_dict[count]['Time']['Hour'] = str(time_subgroup[3])
        time_subgroup = data[subgroup][:]
        #print(time_subgroup)
        time_subgroup.astype('int16').tofile(visited_dir + '/' + str(timestep) + '.bin')
        timestep += 1

    """ We will explore Patorra Clay E """

    data = hf["Results"]["Discharge_Patorra_Clay_E"]
    current_dir = aux_dir + "/Discharge_Patorra_Clay_E/"

    for visited_name in to_visit_groups:
        timestep = aux_timestep
        names_visited = []
        visited = data[visited_name]
        visited.visititems(visitor)
        visited_dir = os.path.join('output', current_dir + visited_name + "/")
        if not os.path.exists(visited_dir):
            os.makedirs(visited_dir)

        for name in names_visited:
            visited_data = []
            visited_data = visited[name][:]
            visited_data.astype('float32').tofile(visited_dir + '/' + str(timestep) + '.bin')
            timestep += 1

    """ We will explore Patorra Clay W """

    data = hf["Results"]["Discharge_Patorra_Clay_W"]
    current_dir = aux_dir + "/Discharge_Patorra_Clay_W/"

    for visited_name in to_visit_groups:
        timestep = aux_timestep
        names_visited = []
        visited = data[visited_name]
        visited.visititems(visitor)
        visited_dir = os.path.join('output', current_dir + visited_name + "/")
        if not os.path.exists(visited_dir):
            os.makedirs(visited_dir)

        for name in names_visited:
            visited_data = []
            visited_data = visited[name][:]
            visited_data.astype('float32').tofile(visited_dir + '/' + str(timestep) + '.bin')
            timestep += 1

    """ We will explore Group_1 - That is both discharges values mixed"""

    data = hf["Results"]["Group_1"]["Data_1D"]
    current_dir = aux_dir + "/Grouped/"

    for visited_name in to_visit_groups:
        timestep = aux_timestep
        names_visited = []
        visited = data[visited_name]
        visited.visititems(visitor)
        visited_dir = os.path.join('output', current_dir + visited_name + "/")
        if not os.path.exists(visited_dir):
            os.makedirs(visited_dir)

        for name in names_visited:
            visited_data = []
            visited_data = visited[name][:]
            visited_data.astype('float32').tofile(visited_dir + '/' + str(timestep) + '.bin')
            timestep += 1

    #saving the last timestep
    aux_timestep = timestep

    #json.dumps(my_dict)
