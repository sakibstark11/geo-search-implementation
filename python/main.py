import math
import random
import time
from typing import List

import h3
from sqlalchemy import Float, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

# --- Configuration ---
RESOLUTION = 8
SEARCH_LAT, SEARCH_LNG = 23.8103, 90.4125
USER_BASE_LAT_MIN, USER_BASE_LAT_MAX = 21.5, 26.3
USER_BASE_LNG_MIN, USER_BASE_LNG_MAX = 88.5, 92.2

# --- Database Setup ---
DB_URL = "sqlite:///../h3_users.db"
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50))
    lat: Mapped[float] = mapped_column(Float)
    lng: Mapped[float] = mapped_column(Float)
    h3_index: Mapped[str] = mapped_column(
        String(15), index=True)  # Index is crucial for performance

    def __repr__(self) -> str:
        return f"<User(name={self.name}, h3={self.h3_index})>"


# Create the tables
Base.metadata.create_all(engine)

# --- Logic ---


def generate_users_batch(n: int, batch_size: int = 10000):
    """Generates users in batches to keep memory usage low."""
    for i in range(0, n, batch_size):
        current_batch_size = min(batch_size, n - i)
        users = []
        for j in range(current_batch_size):
            lat = random.uniform(USER_BASE_LAT_MIN, USER_BASE_LAT_MAX)
            lng = random.uniform(USER_BASE_LNG_MIN, USER_BASE_LNG_MAX)
            h3_hex = h3.latlng_to_cell(lat, lng, RESOLUTION)

            users.append(User(
                name=f"User_{i+j}",
                lat=lat,
                lng=lng,
                h3_index=h3_hex
            ))
        yield users


def populate_database(total_users: int):
    """Populates the SQLite database using bulk inserts."""
    session = SessionLocal()
    try:
        print(f"Generating and inserting {total_users} users...")
        for batch in generate_users_batch(total_users):
            session.add_all(batch)
            session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def find_nearby_users(search_lat: float, search_lng: float, radius_km: int) -> List[User]:
    """
    Finds users within a radius using the H3 Grid Disk optimization.
    Returns a list of User objects.
    """
    session = SessionLocal()

    # 1. Identify the H3 cells that cover the search radius
    center_hex = h3.latlng_to_cell(search_lat, search_lng, RESOLUTION)
    edge_len = h3.average_hexagon_edge_length(RESOLUTION, unit='km')

    # Heuristic for k-ring distance
    k_required = math.ceil(radius_km / (edge_len * 1.5))
    search_hexes = h3.grid_disk(center_hex, k_required)

    # 2. Query only users within those specific H3 cells (highly efficient)
    potential_users = session.query(User).filter(
        User.h3_index.in_(search_hexes)).all()

    # 3. Refine results using actual Great Circle distance
    final_results = [
        u for u in potential_users
        if h3.great_circle_distance((search_lat, search_lng), (u.lat, u.lng)) <= radius_km
    ]

    session.close()
    return final_results

# --- Execution ---


if __name__ == "__main__":
    # Note: Inserting 5 million rows into SQLite might take a few minutes.
    # Reduced to 100k for demonstration purposes; scale up as needed.
    NUM_USERS = 4_000_000

    # Check if DB is empty before populating
    with SessionLocal() as check_session:
        count = check_session.query(User).count()

        if count != NUM_USERS:
            populate_database(NUM_USERS - count)

    radius = 10
    start = time.perf_counter()
    results = find_nearby_users(SEARCH_LAT, SEARCH_LNG, radius)

    print(f"Found {len(results)} users within {radius}km of Dhaka center.")
    print(f"Took {(time.perf_counter() - start) * 1000}ms")
    if results:
        print(
            f"Example result: {results[0].name} at ({results[0].lat}, {results[0].lng})")
