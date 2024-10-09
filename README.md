# Thai Water Data Extraction

## Overview
This project is designed to extract water-related data from the Thai Water API. It processes various datasets, including water levels, water gates, rainfall, and dam data, and saves the results in Markdown format for easy readability and analysis.

## Features
- Fetches water level data, water gate data, rainfall data, and dam data from the Thai Water API.
- Saves the extracted data in Markdown format.
- Implements retry logic with exponential backoff for API requests.
- Validates and enriches data with administrative information based on geographical coordinates.

## Requirements
- Python 3.x
- Required Python packages listed in `requirements.txt`

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/thaiwater-data-extraction.git
   cd thaiwater-data-extraction
   ```

2. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Usage
1. Ensure you have an active internet connection.
2. Run the main script to start the data extraction process:
   ```bash
   python 00-thaiwater-extract-data-v2.py
   ```

3. The output Markdown files will be saved in the `output` directory.

## Output
The following files will be generated in the `output` directory:
- `water_level_station.md`
- `water_level_data.md`
- `water_gate_station.md`
- `water_gate_data.md`
- `rainfall_station.md`
- `rainfall_data.md`
- `dam_station.md`
- `dam_data.md`
- `combined_water_level.md`
- `combined_water_gate.md`
- `combined_rainfall.md`
- `combined_dam.md`

## Logging
The script logs its activities to `data_processing.log`, which can be useful for debugging and tracking the data extraction process.

## Contributing
Contributions are welcome! Please feel free to submit a pull request or open an issue for any suggestions or improvements.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.

## Acknowledgments
- Thanks to the Thai Water API for providing the data.
- Thanks to the open-source community for the libraries used in this project.