import os

async def find_shapefiles(base_dir):
    shapefiles = {}

    async def search_directory(directory):
        shp_files = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith('.shp'):
                    shp_files.append(os.path.join(root, file))
        return shp_files

    for client in os.listdir(base_dir):
        client_path = os.path.join(base_dir, client)
        if os.path.isdir(client_path):
            shapefiles[client] = await search_directory(client_path)
    
    return shapefiles