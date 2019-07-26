from pathlib import Path
from faker import Faker

#: The directory containing our generated example data
DATA_DIRECTORY: Path = Path(__file__).parent

fake = Faker()
