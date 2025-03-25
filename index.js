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

// Extract unique "from" addresses (simplified - no counts)
async function extractFromAddresses(transactions, provider, targetChain) {
  console.log(`Processing ${transactions.length} documents for "from" addresses on ${targetChain}...`);
  
  // Use a Set for unique addresses
  const uniqueAddresses = new Set();
  let processedCount = 0;
  
  for (const doc of transactions) {
    try {
      processedCount++;
      
      // Extract from standard transaction methods
      await processDocument(doc, provider, uniqueAddresses);
      
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

// Process a single document to extract from addresses (simplified - no counts)
async function processDocument(doc, provider, uniqueAddresses) {
  // Helper function to add address
  const addAddress = (address) => {
    if (address) uniqueAddresses.add(address.toLowerCase());
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
async function processChain(db, chain) {
  console.log(`\n========== Processing chain: ${chain} ==========`);
  
  try {
    const provider = getRpcProvider(chain);
    const transactions = await queryTransactions(db, chain);
    
    if (transactions.length === 0) {
      console.log(`No transactions found for ${chain}, skipping...`);
      return [];
    }
    
    const addresses = await extractFromAddresses(transactions, provider, chain);
    console.log(`Completed processing for ${chain}`);
    return addresses;
  } catch (error) {
    console.error(`Error processing chain ${chain}:`, error);
    return []; // Return empty array on error to continue with other chains
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
    for (const chain of ALL_CHAINS) {
      const addresses = await processChain(db, chain);
      allAddressArrays.push(addresses);
    }
    
    // Step 4: Merge addresses from all chains
    const mergedAddresses = mergeAddresses(allAddressArrays);
    console.log(`\nTotal unique addresses across all chains: ${mergedAddresses.length}`);
    
    // Step 5: Write combined output
    await writeAddressesToCsv(mergedAddresses);
    
    console.log('\nProcess completed successfully!');
    process.exit(0);
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
}

// Run the script
main(); 