# Combine to Parquet

Filter Reddit dump files (.zst) and convert to Parquet format.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python combine_to_parquet.py <input_folder> [options]
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--output` | Output Parquet file | `output.parquet` |
| `--field` | Field to filter on | `subreddit` |
| `--value` | Comma-separated values | `pushshift` |
| `--value_list` | File with values (one per line) | None |
| `--processes` | Number of parallel processes | `10` |
| `--batch_size` | Files per batch | `10` |
| `--partial` | Partial string matching | `False` |
| `--regex` | Regex pattern matching | `False` |

### Available Fields

Reddit data contains the following fields that can be used for filtering:

- `author` - Author of the comment/post
- `body` - Text content of the comment
- `subreddit` - Name of the subreddit
- `created_utc` - Creation timestamp (UTC)
- `score` - Number of upvotes/downvotes
- `id` - Unique identifier
- `link_id` - ID of the linked post
- `parent_id` - ID of the parent comment
- `edited` - Edit timestamp
- `archived` - Archive status
- `gilded` - Number of awards received

## Examples

**Filter by subreddit:**
```bash
python scripts/combine_to_parquet.py /path/to/dumps --output python.parquet --value python
```

**Multiple subreddits:**
```bash
python scripts/combine_to_parquet.py /path/to/dumps --value "python,javascript,java"
```

**Using a list file:**
```bash
python scripts/combine_to_parquet.py /path/to/dumps --value_list subreddits.txt
```

**Filter by author:**
```bash
python scripts/combine_to_parquet.py /path/to/dumps --field author --value "username"
```

**Regex matching:**
```bash
python scripts/combine_to_parquet.py /path/to/dumps --value "^(python|java)" --regex
```

**Performance tuning:**
```bash
python scripts/combine_to_parquet.py /path/to/dumps --processes 8 --batch_size 10
```
