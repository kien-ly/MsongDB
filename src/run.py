import h5py
# f = h5py.File("/home/tkien/project/1msong/millionsongsubset/MillionSongSubset/A/A/A/TRAAAAW128F429D538.h5", 'r')

# filename = "/home/tkien/project/1msong/millionsongsubset/MillionSongSubset/A/A/A/TRAAAAW128F429D538.h5"
filename="/home/tkien/project/1msong/millionsongsubset/MillionSongSubset/B/E/D/TRBEDBC128F145B134.h5"
with h5py.File(filename, "r") as f:
    # List all groups
    print("Keys: %s" % f.keys())
    # a_group_key = list(f.keys())[0]
    a_group_key = list(f.keys())
    print(a_group_key)
    for i in range(len(a_group_key)):
        with open('schema.txt', 'a') as fi:               
            print(a_group_key[i])
            fi.writelines("%s\n" % a_group_key[i])
            # fi.writelines('\n'.join(a_group_key))
            # Get the data
            data = list(f[a_group_key[i]])
            # fi.writelines(('\n'.join(data)))
            fi.write("%s\n" % data)
            print(data)

