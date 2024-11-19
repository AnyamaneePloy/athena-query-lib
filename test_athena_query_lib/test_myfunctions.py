# Import the haversine function from the package
from athena_query_lib import haversine

# Test the function
lon1, lat1 = 77.5946, 12.9716  # Bangalore
lon2, lat2 = 72.8777, 19.0760  # Mumbai

distance = haversine(lon1, lat1, lon2, lat2)
print(f"The distance between Bangalore and Mumbai is approximately {distance:.2f} kilometers.")
