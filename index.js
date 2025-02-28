require('dotenv').config();
const { MongoClient } = require('mongodb');
const { ethers } = require('ethers');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// Parse command line arguments
const args = process.argv.slice(2);
const cmdTargetChain = args[0]; // First argument will be the target chain

// Configuration from environment variables with command line override
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
    startTimestamp: parseInt(process.env.START_TIMESTAMP, 10),
    endTimestamp: parseInt(process.env.END_TIMESTAMP, 10),
    // Use command-line arg if provided, otherwise fall back to .env
    targetChain: cmdTargetChain || process.env.TARGET_CHAIN
  },
  output: process.env.OUTPUT_FILE || 'unique_addresses.csv'
};

// Validate configuration
function validateConfig() {
  const requiredVars = [
    'MONGODB_URI', 'MONGODB_DB_NAME', 'START_TIMESTAMP', 
    'END_TIMESTAMP'
  ];

  const missingVars = requiredVars.filter(varName => !process.env[varName]);
  if (missingVars.length > 0) {
    console.error(`Missing required environment variables: ${missingVars.join(', ')}`);
    process.exit(1);
  }

  // Check if target chain is specified
  if (!config.query.targetChain) {
    console.error('No target chain specified. Please provide a chain as a command-line argument or set TARGET_CHAIN in .env file.');
    console.error('Usage: node index.js <chain>');
    console.error('Example: node index.js holesky');
    process.exit(1);
  }

  // Validate RPC URL for target chain exists
  const rpcUrlVar = `${config.query.targetChain.toUpperCase()}_RPC_URL`;
  if (!process.env[rpcUrlVar]) {
    console.error(`Missing RPC URL for chain ${config.query.targetChain}. Please set ${rpcUrlVar} in .env file.`);
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

// Get RPC provider for the target chain
function getRpcProvider(chain) {
  const rpcUrl = config.rpc[chain.toLowerCase()];
  if (!rpcUrl) {
    throw new Error(`No RPC URL configured for chain: ${chain}`);
  }
  return new ethers.JsonRpcProvider(rpcUrl);
}

// Query transactions from MongoDB
async function queryTransactions(db) {
  try {
    const collection = db.collection('venn-guard-txs');
    
    const query = {
      timestamp: {
        $gte: config.query.startTimestamp,
        $lte: config.query.endTimestamp
      },
      target: config.query.targetChain
    };

    console.log(`Querying transactions with criteria:`, query);
    
    const transactions = await collection.find(query).toArray();
    console.log(`Found ${transactions.length} transactions matching criteria`);
    return transactions;
  } catch (error) {
    console.error('Error querying transactions:', error);
    throw error;
  }
}

// Combined function to extract all "from" addresses
async function extractFromAddresses(transactions, provider) {
  console.log(`Processing ${transactions.length} documents for "from" addresses...`);
  
  // Change from Set to Map to track transaction counts
  const addressTransactionCounts = new Map();
  const methodCounts = {};
  let processedCount = 0;
  
  for (const doc of transactions) {
    try {
      processedCount++;
      methodCounts[doc.method] = (methodCounts[doc.method] || 0) + 1;
      
      // Extract from standard transaction methods
      await processDocument(doc, provider, addressTransactionCounts);
      
      // Log progress less frequently
      if (processedCount % 50 === 0) {
        console.log(`Processed ${processedCount}/${transactions.length} documents. Found ${addressTransactionCounts.size} unique "from" addresses so far.`);
      }
    } catch (error) {
      console.error(`Error processing document ${processedCount}:`, error);
    }
  }
  
  console.log(`\nMethod counts:`, methodCounts);
  console.log(`Processed ${processedCount} total documents`);
  console.log(`Extracted ${addressTransactionCounts.size} unique "from" addresses`);
  
  // Convert Map to array of objects with address and count
  return Array.from(addressTransactionCounts.entries()).map(([address, count]) => {
    return { address, count };
  });
}

// Process a single document to extract from addresses - updated to increment counts
async function processDocument(doc, provider, addressCounts) {
  // Helper function to add address with count
  const addAddress = (address) => {
    const lowerAddress = address.toLowerCase();
    addressCounts.set(lowerAddress, (addressCounts.get(lowerAddress) || 0) + 1);
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

// Write addresses to CSV - updated to include transaction count
async function writeAddressesToCsv(addressData) {
  const filename = `${config.query.targetChain}_${config.output}`;
  
  const csvWriter = createCsvWriter({
    path: filename,
    header: [
      { id: 'address', title: 'ADDRESS' },
      { id: 'count', title: 'TRANSACTION_COUNT' }
    ]
  });
  
  // Sort by transaction count (highest first)
  const sortedAddresses = addressData.sort((a, b) => b.count - a.count);
  
  await csvWriter.writeRecords(sortedAddresses);
  console.log(`CSV file created successfully at ${filename}`);
  
  // Print top addresses by transaction count
  console.log('\nTop 5 addresses by transaction count:');
  sortedAddresses.slice(0, 5).forEach((item, index) => {
    console.log(`${index + 1}. ${item.address}: ${item.count} transactions`);
  });
}

// Main function
async function main() {
  try {
    // Step 1: Validate configuration
    validateConfig();
    
    console.log(`Target chain: ${config.query.targetChain}`);
    
    // Step 2: Setup connections
    const db = await connectToMongoDB();
    const provider = getRpcProvider(config.query.targetChain);
    
    // Step 3: Query transactions
    const transactions = await queryTransactions(db);
    
    // Step 4: Extract addresses
    const fromAddresses = await extractFromAddresses(transactions, provider);
    
    // Step 5: Write output
    await writeAddressesToCsv(fromAddresses);
    
    console.log('Process completed successfully!');
    process.exit(0);
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
}

// Run the script
main(); 