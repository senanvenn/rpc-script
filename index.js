require('dotenv').config();
const { MongoClient } = require('mongodb');
const { ethers } = require('ethers');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const fs = require('fs');

// List of all chains to process
const ALL_CHAINS = ['bnb', 'mainnet', 'bnbtestnet', 'holesky'];

// Configuration from environment variables
const config = {
  mongodb: {
    uri: process.env.MONGODB_URI,
    dbName: process.env.MONGODB_DB_NAME
  },
  rpc: {
    bnb: process.env.BNB_RPC_URL,
    holesky: process.env.HOLESKY_RPC_URL,
    bnbtestnet: process.env.BNBTESTNET_RPC_URL,
    mainnet: process.env.MAINNET_RPC_URL
  },
  query: {
    startTimestamp: parseInt(process.env.START_TIMESTAMP, 10)
  },
  output: process.env.OUTPUT_FILE || 'all_chains_unique_addresses.csv'
};

// Validate configuration
function validateConfig() {
  const requiredVars = [
    'MONGODB_URI', 'MONGODB_DB_NAME', 'START_TIMESTAMP'
  ];

  const missingVars = requiredVars.filter(varName => !process.env[varName]);
  if (missingVars.length > 0) {
    console.error(`Missing required environment variables: ${missingVars.join(', ')}`);
    process.exit(1);
  }

  // Validate RPC URLs for all chains
  const missingRpcUrls = [];
  for (const chain of ALL_CHAINS) {
    const rpcUrlVar = `${chain.toUpperCase()}_RPC_URL`;
    if (!process.env[rpcUrlVar]) {
      missingRpcUrls.push(rpcUrlVar);
    }
  }
  
  if (missingRpcUrls.length > 0) {
    console.error(`Missing RPC URLs for chains: ${missingRpcUrls.join(', ')}`);
    process.exit(1);
  }
}

// Connect to MongoDB
async function connectToMongoDB() {
  try {
    const client = new MongoClient(config.mongodb.uri);
    await client.connect();
    console.log('Connected to MongoDB');
    return client.db(config.mongodb.dbName);
  } catch (error) {
    console.error('Failed to connect to MongoDB:', error);
    throw error;
  }
}

// Get RPC provider for a specific chain
function getRpcProvider(chain) {
  const rpcUrl = config.rpc[chain.toLowerCase()];
  if (!rpcUrl) {
    throw new Error(`No RPC URL configured for chain: ${chain}`);
  }
  return new ethers.JsonRpcProvider(rpcUrl);
}

// Query transactions from MongoDB for a specific chain
async function queryTransactions(db, targetChain) {
  try {
    const collection = db.collection('venn-guard-txs');
    
    const query = {
      timestamp: { $gte: config.query.startTimestamp },
      target: targetChain
    };
    
    const humanStartDate = new Date(config.query.startTimestamp * 1000).toLocaleString();
    console.log(`\nQuerying transactions for chain ${targetChain}`);
    console.log(`Finding all transactions from ${humanStartDate} (${config.query.startTimestamp}) until present`);
    
    const transactions = await collection.find(query).toArray();
    console.log(`Found ${transactions.length} transactions matching criteria for ${targetChain}`);
    return transactions;
  } catch (error) {
    console.error(`Error querying transactions for ${targetChain}:`, error);
    throw error;
  }
}

// Extract unique "from" addresses (with transaction counts)
async function extractFromAddresses(transactions, provider, targetChain, addressCounts) {
  console.log(`Processing ${transactions.length} documents for "from" addresses on ${targetChain}...`);
  
  // Use a Set for unique addresses
  const uniqueAddresses = new Set();
  let processedCount = 0;
  
  for (const doc of transactions) {
    try {
      processedCount++;
      
      // Extract from standard transaction methods
      await processDocument(doc, provider, uniqueAddresses, addressCounts);
      
      // Log progress less frequently
      if (processedCount % 100 === 0) {
        console.log(`Processed ${processedCount}/${transactions.length} documents on ${targetChain}. Found ${uniqueAddresses.size} unique addresses.`);
      }
    } catch (error) {
      console.error(`Error processing document ${processedCount} on ${targetChain}:`, error);
    }
  }
  
  console.log(`Processed ${processedCount} total documents on ${targetChain}`);
  console.log(`Extracted ${uniqueAddresses.size} unique "from" addresses from ${targetChain}`);
  
  // Return array of unique addresses
  return Array.from(uniqueAddresses);
}

// Process a single document to extract from addresses (with transaction counts)
async function processDocument(doc, provider, uniqueAddresses, addressCounts) {
  // Helper function to add address and update counts
  const addAddress = (address) => {
    if (address) {
      const lowerAddress = address.toLowerCase();
      uniqueAddresses.add(lowerAddress);
      
      if (addressCounts[lowerAddress]) {
        addressCounts[lowerAddress]++;
      } else {
        addressCounts[lowerAddress] = 1;
      }
    }
  };

  // CASE 1: Block with transactions
  if ((doc.method === 'eth_getBlockByNumber' || doc.method === 'eth_getBlockByHash') && 
      Array.isArray(doc.response?.body?.result?.transactions)) {
    const txs = doc.response.body.result.transactions;
    
    if (txs.length > 0 && typeof txs[0] === 'object') {
      // Full transaction objects
      for (const tx of txs) {
        if (tx.from) addAddress(tx.from);
      }
    } else {
      // Transaction hashes
      for (const txHash of txs) {
        try {
          const txDetails = await provider.getTransaction(txHash);
          if (txDetails?.from) addAddress(txDetails.from);
        } catch (error) {
          // Silent fail for individual tx errors
        }
      }
    }
  }
  
  // CASE 2: Direct transaction methods
  else if (doc.method === 'eth_sendRawTransaction' && doc.response?.body?.result) {
    // Get the transaction hash from the response
    const txHash = doc.response.body.result;
    try {
      const txDetails = await provider.getTransaction(txHash);
      if (txDetails?.from) addAddress(txDetails.from);
    } catch (error) {
      // Silent fail for individual tx errors
    }
  }
  else if (doc.method === 'eth_getTransactionByHash' && doc.response?.body?.result?.from) {
    addAddress(doc.response.body.result.from);
  }
  else if (doc.method === 'eth_getTransactionReceipt') {
    // Get the transaction hash from request params
    const txHash = doc.request?.body?.params?.[0];
    if (txHash) {
      try {
        const txDetails = await provider.getTransaction(txHash);
        if (txDetails?.from) addAddress(txDetails.from);
      } catch (error) {
        // Silent fail for individual tx errors
      }
    }
  }
  
  // CASE 3: From fields in request parameters
  if (doc.request?.body?.params) {
    const params = doc.request.body.params;
    if (Array.isArray(params)) {
      for (const param of params) {
        if (param && typeof param === 'object' && param.from) {
          addAddress(param.from);
        }
      }
    }
  }
  
  // CASE 4: Method-specific parameter patterns
  if ((doc.method === 'eth_call' || doc.method === 'eth_estimateGas') && 
      doc.request?.body?.params?.[0]?.from) {
    addAddress(doc.request.body.params[0].from);
  }
}

// Merge address arrays from multiple chains
function mergeAddresses(allAddressArrays) {
  // Combine all addresses into one Set to ensure uniqueness
  const allUniqueAddresses = new Set();
  
  for (const addressArray of allAddressArrays) {
    for (const address of addressArray) {
      allUniqueAddresses.add(address);
    }
  }
  
  return Array.from(allUniqueAddresses);
}

// Write addresses to CSV - simplified to just one column
async function writeAddressesToCsv(addresses) {
  const filename = config.output;
  
  const csvWriter = createCsvWriter({
    path: filename,
    header: [
      { id: 'address', title: 'ADDRESS' }
    ]
  });
  
  const records = addresses.map(address => ({ address }));
  
  await csvWriter.writeRecords(records);
  console.log(`\nCSV file created successfully at ${filename}`);
  console.log(`Total unique addresses: ${addresses.length}`);
}

// Process a single chain
async function processChain(db, chain, addressCounts) {
  console.log(`\n========== Processing chain: ${chain} ==========`);
  
  try {
    const provider = getRpcProvider(chain);
    const transactions = await queryTransactions(db, chain);

    if (transactions.length === 0) {
      console.log(`No transactions found for ${chain}, skipping...`);
      return { addresses: [], transactionCount: 0 };
    }

    const addresses = await extractFromAddresses(transactions, provider, chain, addressCounts);
    console.log(`Completed processing for ${chain}`);
    return { addresses, transactionCount: transactions.length };
  } catch (error) {
    console.error(`Error processing chain ${chain}:`, error);
    return { addresses: [], transactionCount: 0 }; // Return empty data on error to continue with other chains
  }
}

// Main function
async function main() {
  try {
    // Step 1: Validate configuration
    validateConfig();
    
    // Step 2: Setup MongoDB connection
    const db = await connectToMongoDB();
    
    // Step 3: Process each chain
    console.log(`Processing ${ALL_CHAINS.length} chains: ${ALL_CHAINS.join(', ')}`);
    
    const allAddressArrays = [];
    let totalTransactions = 0; // Track total transactions
    const transactionsByChain = {}; // Track transactions by chain
    const addressCounts = {}; // Initialize address counts map
    
    for (const chain of ALL_CHAINS) {
      const { addresses, transactionCount } = await processChain(db, chain, addressCounts);
      allAddressArrays.push(addresses);
      transactionsByChain[chain] = transactionCount;
      totalTransactions += transactionCount;
    }
    
    // Step 4: Merge addresses from all chains
    const mergedAddresses = mergeAddresses(allAddressArrays);
    console.log(`\nTotal unique addresses across all chains: ${mergedAddresses.length}`);
    console.log(`Total transactions across all chains: ${totalTransactions}`);
    
    // Log breakdown by chain
    console.log("\nBreakdown by chain:");
    for (const chain of ALL_CHAINS) {
      console.log(`${chain}: ${transactionsByChain[chain]} transactions`);
    }
    
    // Convert timestamp to human-readable date
    const startDate = new Date(config.query.startTimestamp * 1000).toLocaleString();
    console.log(`\nTime period: From ${startDate} to now`);
    
    // Step 5: Write combined address output
    await writeAddressesToCsv(mergedAddresses);
    
    // Step 6: Write transaction statistics to a separate file
    await writeTransactionStatsToCsv(mergedAddresses.length, totalTransactions, transactionsByChain, startDate);
    
    // Step 7: Write address transaction counts to a separate CSV
    await writeAddressTransactionCountsToCsv(addressCounts);
    
    console.log('\nProcess completed successfully!');
    process.exit(0);
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
}

// New function to write transaction statistics to a separate CSV
async function writeTransactionStatsToCsv(walletCount, totalTransactions, transactionsByChain, startDate) {
  // Create filename by replacing .csv with _stats.csv
  const baseFilename = config.output.replace(/\.csv$/, '');
  const statsFilename = `${baseFilename}_stats.csv`;
  
  // Initialize CSV writer for statistics
  const csvWriter = createCsvWriter({
    path: statsFilename,
    header: [
      { id: 'metric', title: 'METRIC' },
      { id: 'value', title: 'VALUE' }
    ]
  });
  
  // Prepare records for the stats CSV
  const records = [
    { metric: 'Time Period Start', value: startDate },
    { metric: 'Time Period End', value: new Date().toLocaleString() },
    { metric: 'Total Unique Wallets', value: walletCount },
    { metric: 'Total Transactions', value: totalTransactions },
  ];
  
  // Add per-chain transaction counts
  for (const chain of ALL_CHAINS) {
    records.push({
      metric: `Transactions on ${chain}`,
      value: transactionsByChain[chain] || 0
    });
  }
  
  await csvWriter.writeRecords(records);
  console.log(`\nTransaction statistics CSV created successfully at ${statsFilename}`);
}

// New function to write address transaction counts to a separate CSV
async function writeAddressTransactionCountsToCsv(addressCounts) {
  // Create filename by appending _counts to the original output filename
  const baseFilename = config.output.replace(/\.csv$/, '');
  const countsFilename = `${baseFilename}_counts.csv`;
  
  // Initialize CSV writer for address transaction counts
  const csvWriter = createCsvWriter({
    path: countsFilename,
    header: [
      { id: 'address', title: 'ADDRESS' },
      { id: 'transactionCount', title: 'TRANSACTION_COUNT' }
    ]
  });
  
  // Prepare records for the counts CSV
  const records = Object.keys(addressCounts).map(address => ({
    address,
    transactionCount: addressCounts[address]
  }));
  
  await csvWriter.writeRecords(records);
  console.log(`\nAddress transaction counts CSV created successfully at ${countsFilename}`);
}

// Run the script
main(); 