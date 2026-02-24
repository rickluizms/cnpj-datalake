import argparse
import asyncio
from dotenv import load_dotenv

load_dotenv()

def run_ingestion():
    from src.ingest.main import run as ingest_main
    ingest_main()

def run_transform():
    from src.transform.main import main as transform_main
    transform_main()

parser = argparse.ArgumentParser(description="CNPJ Datalake")
parser.add_argument("--ingest", action="store_true", help="Run ingestion")
parser.add_argument("--transform", action="store_true", help="Run transform")
parser.add_argument("--all", action="store_true", help="Run all")
parser.add_argument("--version", action="version", version="%(prog)s 1.0.0", help="Show version")

def run_help():
    print("Usage: python main.py [options]")
    print("Options:")
    print("  --ingest    Run ingestion")
    print("  --transform Run transform")
    print("  --all       Run all")
    print("  --version   Show version")

def parse_args():
    args = parser.parse_args()
    if args.ingest:
        run_ingestion()
    elif args.transform:
        run_transform()
    elif args.all:
        run_ingestion()
        run_transform()
    else:
        print("CNPJ Datalake pipeline")

if __name__ == "__main__":
    parse_args()