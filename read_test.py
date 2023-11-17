import time
import random
import database

def main():
    db = database.DB("test")

    for i in range(10):
        random_id = random.randint(1, 100000)
        start = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
        db.search("users", "id", random_id)
        end = time.clock_gettime_ns(time.CLOCK_MONOTONIC)

        print(f"search took {end - start} ns")
        time.sleep(1)

if __name__ == "__main__":
    main()