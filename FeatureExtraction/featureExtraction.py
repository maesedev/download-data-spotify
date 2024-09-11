import librosa
import numpy as np
import pandas as pd
import os
import inquirer
import logging
from multiprocessing import Pool, cpu_count
import shutil


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Global Variables
AUDIO_DIRECTORY = "../__songs__/"
CSV_FILE = "audio_features.csv"
processed_dir = "../__songs_processed__"

# Define columns for the CSV file
COLUMNS = (
    ["name", "rms", "zcr", "tempo", "onset_strength"]
    + [f"mfcc_{i}" for i in range(1, 14)]
    + [f"spectral_contrast_{i}" for i in range(1, 8)]
)

# Load existing CSV or create a new DataFrame
try:
    logging.info(f"Attempting to load CSV file: {CSV_FILE}")
    songs_csv = pd.read_csv(CSV_FILE)
    logging.info(f"Successfully loaded CSV file: {CSV_FILE}")
except FileNotFoundError:
    logging.info(f"No processed songs found at {CSV_FILE}. Creating a new CSV file.")
    songs_csv = pd.DataFrame(columns=COLUMNS)


def extract_features(audio_file):
    """
    Extract features from an audio file using librosa.
    """
    audio_name = os.path.basename(audio_file).replace(".mp3", "")

    # Set 'name' column as index for faster lookup (if not already set)
    if songs_csv.index.name != "name":
        songs_csv.set_index("name", inplace=True)

    if audio_name in songs_csv.index:
        if not os.path.exists(processed_dir):
            os.makedirs(processed_dir)
            logging.info(f"Created directory: {processed_dir}")

        logging.info(f"Moving to {processed_dir}/{audio_name}.mp3: Already processed")
        shutil.move(audio_file, f"{processed_dir}/{audio_name}.mp3")
        return None, audio_name

    try:
        # Load the audio file
        y, sr = librosa.load(audio_file, sr=None)

        # Check the duration of the audio file
        duration = librosa.get_duration(y=y, sr=sr)

        # If the duration is longer than 10 minutes, delete the file and continue
        if duration > 600:  # 600 seconds = 10 minutes
            os.remove(audio_file)
            print(f"Deleted {audio_file} because it is longer than 10 minutes.")
            return None, audio_name

        # Extract features
        rms = np.mean(librosa.feature.rms(y=y))
        zcr = np.mean(librosa.feature.zero_crossing_rate(y=y))
        tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
        spectral_contrast = np.mean(
            librosa.feature.spectral_contrast(y=y, sr=sr), axis=1
        )
        onset_strength = np.mean(librosa.onset.onset_strength(y=y, sr=sr))
        mfcc = np.mean(librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13), axis=1)

        logging.info(f"Processed song: {audio_name}")

        # Return the list of features
        return (
            [audio_name, rms, zcr, tempo, onset_strength]
            + mfcc.tolist()
            + spectral_contrast.tolist()
        ), audio_name

    except Exception as e:
        logging.error(f"Error processing {audio_file}: {e}")
        return None, audio_name


def process_song(audio_file):
    """
    Process a song to extract its features and save the results to a CSV file.
    """
    logging.info(f"Processing song: {audio_file}")
    features, audio_name = extract_features(audio_file)
    if features:
        logging.info(f"Moving to {processed_dir}/{audio_name}.mp3")
        shutil.move(audio_file, f"{processed_dir}/{audio_name}.mp3")
        df_features = pd.DataFrame([features], columns=COLUMNS)
        df_features.to_csv(CSV_FILE, mode="a", header=False, index=False)


def main():
    """
    Main entry point of the program.
    Uses multiprocessing to extract features in parallel from multiple .mp3 files.
    The features are saved incrementally to a CSV file.
    """
    try:
        logging.info("Starting process")
        questions = [
            inquirer.List(
                "processing_force",
                message="What percentage of CPU will you use?",
                choices=["30%", "50%", "80%", "100%"],
            )
        ]

        processing_force = inquirer.prompt(questions)["processing_force"][:-1]
        processing_force = int(processing_force) / 100

        # Get the number of available CPUs
        num_cpus = cpu_count()
        max_workers = round(num_cpus * processing_force)

        logging.info(f"Number of CPUs: {num_cpus}")
        logging.info(f"Number of workers: {max_workers}")

        # Write headers before processing
        songs_csv.to_csv(CSV_FILE, mode="w", index=False)

        # Get list of audio files
        audio_files = [
            os.path.join(AUDIO_DIRECTORY, f)
            for f in os.listdir(AUDIO_DIRECTORY)
            if f.endswith(".mp3")
        ]

        # Create a pool of workers to process multiple audio files in parallel
        with Pool(max_workers) as pool:
            pool.map(process_song, audio_files)

    except KeyboardInterrupt:
        logging.warning("Process interrupted. Progress saved up to this point.")

    except Exception as e:
        logging.error(f"Execution error: {e}")

    finally:
        logging.info(f"Features saved to {CSV_FILE}")


if __name__ == "__main__":
    main()
