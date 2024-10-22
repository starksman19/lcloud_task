import boto3
import re
import argparse
from dotenv import load_dotenv


load_dotenv()


def get_s3_client():
    return boto3.client("s3")


def list_files(s3_client, bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="b-wing/")
    if "Contents" in response:
        for obj in response["Contents"]:
            print(obj["Key"])
    else:
        print("No files found or bucket is empty")


def upload_file(s3_client, bucket_name, local_file, s3_key):
    try:
        s3_key_in_b_wing = (
            f"b-wing/{s3_key}"
        )
        s3_client.upload_file(local_file, bucket_name, s3_key_in_b_wing)
        print(f"Uploaded {local_file} to s3")
    except Exception as e:
        print(f"Failed to upload: {str(e)}")


def list_files_matching_regex(s3_client, bucket_name, regex):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="b-wing/")
    pattern = re.compile(regex)
    if "Contents" in response:
        matching_files = [
            obj["Key"] for obj in response["Contents"] if pattern.search(obj["Key"])
        ]
        if matching_files:
            print(f"Matching files: {matching_files}")
        else:
            print("No files match the regex")
    else:
        print("No files found or bucket is empty")


def delete_files_matching_regex(s3_client, bucket_name, regex):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="b-wing/")
    pattern = re.compile(regex)
    if "Contents" in response:
        matching_files = [
            obj["Key"] for obj in response["Contents"] if pattern.search(obj["Key"])
        ]
        if matching_files:
            for file_key in matching_files:
                s3_client.delete_object(Bucket=bucket_name, Key=file_key)
                print(f"Deleted {file_key} from {bucket_name}")
        else:
            print("No files match the regex")
    else:
        print("No files found or bucket is empty")


# CLI using argparse
def main():
    parser = argparse.ArgumentParser(description="AWS S3 script")
    subparsers = parser.add_subparsers(
        dest="command", help="Subcommands: list, upload, regex-list, regex-delete"
    )

    # List files
    subparsers.add_parser("list", help="List all files of the bucket")

    # Upload
    upload_parser = subparsers.add_parser("upload", help="Upload a file to S3")
    upload_parser.add_argument("--local-file", required=True, help="Path to local file")
    upload_parser.add_argument(
        "--s3-key",
        required=True,
        help="S3 key (filename) to upload",
    )

    # List regex
    regex_list_parser = subparsers.add_parser(
        "regex-list", help="List files matching a regex"
    )
    regex_list_parser.add_argument(
        "--regex", required=True, help="Regex to match file names"
    )

    # Delete regex
    regex_delete_parser = subparsers.add_parser(
        "regex-delete", help="Delete files matching a regex"
    )
    regex_delete_parser.add_argument(
        "--regex", required=True, help="Regex files for deletion"
    )

    args = parser.parse_args()
    s3_client = get_s3_client()
    bucket = "developer-task"

    if args.command == "list":
        list_files(s3_client, bucket)
    elif args.command == "upload":
        upload_file(s3_client, bucket, args.local_file, args.s3_key)
    elif args.command == "regex-list":
        list_files_matching_regex(s3_client, bucket, args.regex)
    elif args.command == "regex-delete":
        delete_files_matching_regex(s3_client, bucket, args.regex)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
