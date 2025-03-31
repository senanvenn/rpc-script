# RPC Script

A Node.js utility for extracting and analyzing unique wallet addresses from blockchain transactions across multiple chains.

## Overview

This script queries transaction data from MongoDB, extracts wallet addresses, and generates CSV reports with:
- Unique wallet addresses across all chains
- Transaction counts per address
- Statistical summaries
- Intersection analysis with provided address lists

## Supported Chains

- Ethereum Mainnet
- BNB Chain
- BNB Testnet
- Holesky (Ethereum testnet)

## Prerequisites

- Node.js (v14 or higher)
- MongoDB instance with transaction data
- RPC endpoints for each blockchain

## Installation

1. Clone the repository
2. Install dependencies:
   ```
   npm install
   ```
3. Copy the example environment file:
   ```
   cp .env.example .env
   ```
4. Configure your environment variables in `.env`

## Configuration

Edit the `.env` file with the following required settings:

```
# MongoDB Configuration
MONGODB_URI=your_mongodb_connection_string
MONGODB_DB_NAME=your_database_name

# Blockchain RPC Node Configuration
MAINNET_RPC_URL=your_ethereum_mainnet_rpc_url
BNB_RPC_URL=your_bnb_chain_rpc_url
BNBTESTNET_RPC_URL=your_bnb_testnet_rpc_url
HOLESKY_RPC_URL=your_holesky_testnet_rpc_url

# Query Parameters
START_TIMESTAMP=1740178800  # Unix timestamp in seconds

# Output Configuration
OUTPUT_FILE=unique_addresses.csv
```

## Usage

Run the script with:

```
node index.js
```

## Output Files

The script generates the following CSV files:

1. `unique_addresses.csv` - List of all unique wallet addresses
2. `unique_addresses_counts.csv` - Addresses with transaction counts
3. `unique_addresses_stats.csv` - Statistical summary of transactions and addresses
4. `unique_addresses_intersection.csv` - Addresses found in both the results and `intract.csv`

## Example

When you run the script, it will:

1. Connect to the MongoDB database specified in your `.env` file
2. Query transactions since the specified timestamp
3. Process transactions across all chains
4. Extract unique wallet addresses
5. Generate the CSV reports
6. Show summary statistics in the console

## Notes

- The script expects a MongoDB collection named `venn-guard-txs`
- Set the `START_TIMESTAMP` to filter transactions from that timestamp onward
- Include an `intract.csv` file with addresses to find intersections 