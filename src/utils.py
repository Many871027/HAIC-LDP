import numpy as np

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    TODO: Implementar la fórmula de Haversine para calcular la distancia del círculo máximo 
    entre dos puntos de la Tierra (especificados en grados decimales).
    
    Esta función debe recibir latitud/longitud del comprador y del vendedor y devolver
    la distancia en kilómetros (km).
    
    Fórmula matemática:
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin²(dlat/2) + cos(lat1) * cos(lat2) * sin²(dlon/2)
        c = 2 * arcsin(√a)
        r = 6371  # Radio de la Tierra en kilómetros
        distancia = c * r
        
    Nota: Recuerda convertir los grados decimales a radianes usando np.radians().
    """
    # Escribe tu código aquí:
    pass
