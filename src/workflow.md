# Advanced RAG Pipeline - Architecture & Workflow Specifications

This document outlines the detailed system architecture, component workflows, data pipelines, and sequence flows for the Advanced Retrieval-Augmented Generation (RAG) system built with **Express**, **BullMQ**, **Redis**, **Qdrant**, and **OpenAI**.

---

## 1. System Architecture Overview

The system is decoupled into an asynchronous job producer/consumer architecture:
- **API Server (`src/index.js`)**: Ingests document uploads and query requests, enqueueing them as background jobs.
- **Message Broker (`src/queue.js`)**: Redis-backed BullMQ queues (`file-indexing` and `query`) for resilient job scheduling and state management.
- **Worker Service (`src/worker.js`)**: Consumes jobs concurrently from Redis to execute heavy text parsing, embedding generation, and vector retrieval tasks.
- **Indexing Engine (`src/indexer.js`)**: Extracts text from PDFs using `pdf-parse`, chunks content with sliding window overlap, embeds text via OpenAI `text-embedding-3-small`, and upserts into Qdrant.
- **Advanced Retrieval Engine (`src/retriever.js`)**: Performs Query Expansion (Step-Back Prompting, Query Rewriting, Sub-Query Decomposition), HyDE (Hypothetical Document Embeddings), parallel multi-query vector searches, and Reciprocal Rank Fusion (RRF).
- **Vector Storage (`src/qdrant.js`)**: Qdrant Vector Database configured for 1536-dimensional Cosine similarity search.
- **LLM / Embedding Services (`src/openai.js`)**: OpenAI API interface for text embeddings and `gpt-4o-mini` chat completions.

---

## 2. High-Level Master System Architecture

The diagram below illustrates the end-to-end component topology, data flows, and external service integrations across the entire pipeline.

```mermaid
flowchart TB
    %% Styling Classes
    classDef client fill:#e1f5fe,stroke:#0288d1,stroke-width:2px,color:#01579b;
    classDef api fill:#fff3e0,stroke:#f57c00,stroke-width:2px,color:#e65100;
    classDef queue fill:#ede7f6,stroke:#7b1fa2,stroke-width:2px,color:#4a148c;
    classDef worker fill:#e8f5e9,stroke:#388e3c,stroke-width:2px,color:#1b5e20;
    classDef engine fill:#fce4ec,stroke:#c2185b,stroke-width:2px,color:#880e4f;
    classDef db fill:#e0f2f1,stroke:#00796b,stroke-width:2px,color:#004d40;
    classDef external fill:#f3e5f5,stroke:#8e24aa,stroke-width:2px,color:#4a148c;

    subgraph CLIENT_LAYER ["Client Layer"]
        User["Client / HTTP Requestor"]:::client
    end

    subgraph API_GATEWAY ["Express API Layer (src/index.js)"]
        POST_Index["POST /index\n(Multer PDF Upload)"]:::api
        POST_Query["POST /query\n(JSON Query Payload)"]:::api
        GET_Poll["GET /query/:id\n(Job Status & Result Polling)"]:::api
    end

    subgraph QUEUE_LAYER ["Distributed Queue Layer (src/queue.js + Redis)"]
        IndexingQueue[("BullMQ: file-indexing Queue\n(Concurrency: 2)")]:::queue
        QueryQueue[("BullMQ: query Queue\n(Concurrency: 4)")]:::queue
    end

    subgraph WORKER_LAYER ["Background Worker Service (src/worker.js)"]
        IndexingWorker["Indexing Worker\n(indexPdf Task)"]:::worker
        QueryWorker["Query Worker\n(answerQuery Task)"]:::worker
    end

    subgraph CORE_MODULES ["Core Processing Modules"]
        IndexerEngine["Indexing Engine\n(src/indexer.js)"]:::engine
        RetrieverEngine["Retrieval & RAG Engine\n(src/retriever.js)"]:::engine
    end

    subgraph DATA_STORAGE ["Storage & External Services"]
        UploadDir[("Local Disk\n/uploads")]:::db
        QdrantDB[("Qdrant Vector DB\nCollection: 'documents'\n(1536 Cosine Vector)")]:::db
        OpenAI_API["OpenAI API Service\n(Embeddings & gpt-4o-mini)"]:::external
    end

    %% Interactions
    User -->|"1. Upload PDF"| POST_Index
    User -->|"2. Submit Query"| POST_Query
    User -->|"3. Poll Job Status"| GET_Poll

    POST_Index -->|"Save to disk"| UploadDir
    POST_Index -->|"Enqueue Indexing Job"| IndexingQueue
    POST_Query -->|"Enqueue Query Job"| QueryQueue
    GET_Poll -->|"Check Job State / Result"| QueryQueue

    IndexingQueue -->|"Pop Job"| IndexingWorker
    QueryQueue -->|"Pop Job"| QueryWorker

    IndexingWorker -->|"Invoke indexPdf()"| IndexerEngine
    QueryWorker -->|"Invoke answerQuery()"| RetrieverEngine

    IndexerEngine -->|"Read PDF File"| UploadDir
    IndexerEngine -->|"Batch Embed Chunks"| OpenAI_API
    IndexerEngine -->|"Upsert Vectors & Payload"| QdrantDB

    RetrieverEngine -->|"Query Expansion & HyDE"| OpenAI_API
    RetrieverEngine -->|"Vector Search Query"| QdrantDB
    RetrieverEngine -->|"Grounded Completion"| OpenAI_API
```

---

## 3. Asynchronous PDF Ingestion & Indexing Pipeline

This flowchart outlines the detailed internal sequence when a document is uploaded, chunked, embedded, and indexed into Qdrant.

```mermaid
flowchart TD
    classDef startEnd fill:#424242,stroke:#212121,stroke-width:2px,color:#ffffff;
    classDef process fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1;
    classDef decision fill:#fff9c4,stroke:#fbc02d,stroke-width:2px,color:#f57f17;
    classDef external fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#4a148c;

    Start(["Upload Request: POST /index"]):::startEnd
    Multer["Multer Middleware:\nValidates mimetype (application/pdf)\nStores in /uploads with unique UUID filename"]:::process
    EnqueueIndex["Enqueue job to BullMQ 'file-indexing'\nReturns 202 Accepted { jobId }"]:::process
    WorkerPickup["Indexing Worker picks up job from Queue"]:::process
    EnsureColl["ensureCollection():\nCheck if Qdrant collection exists\nCreate with size=1536, distance=Cosine if missing"]:::process
    ReadPDF["readPdfText():\nRead file buffer via fs\nParse text with pdf-parse"]:::process
    Chunking["chunkText():\nClean whitespace\nSliding window (~1000 chars, 200 overlap)\nBreak on space boundaries"]:::process
    HasText{"Extractable Chunks > 0?"}:::decision
    EmptyErr["Return { chunks: 0, message: 'No extractable text' }"]:::process
    BatchEmbed["embedTexts():\nBatch chunks into groups of 100\nRequest OpenAI text-embedding-3-small"]:::external
    PreparePoints["Construct Qdrant Points:\n- Point ID: UUID\n- Vector: 1536-dim array\n- Payload: { text, source, filePath, chunkIndex }"]:::process
    UpsertQdrant["qdrant.upsert():\nUpsert points into collection\nWait for completion signal"]:::external
    Finish(["Indexing Complete\nJob marked 'completed' in BullMQ"]):::startEnd

    Start --> Multer
    Multer --> EnqueueIndex
    EnqueueIndex --> WorkerPickup
    WorkerPickup --> EnsureColl
    EnsureColl --> ReadPDF
    ReadPDF --> Chunking
    Chunking --> HasText
    HasText -- "No" --> EmptyErr
    HasText -- "Yes" --> BatchEmbed
    BatchEmbed --> PreparePoints
    PreparePoints --> UpsertQdrant
    UpsertQdrant --> Finish
```

---

## 4. Advanced Multi-Query & HyDE Retrieval Workflow

The system supports advanced multi-query expansion, HyDE passage generation, parallel vector search, and Reciprocal Rank Fusion (RRF) to maximize retrieval precision and recall.

```mermaid
flowchart TD
    classDef startEnd fill:#37474f,stroke:#263238,stroke-width:2px,color:#ffffff;
    classDef prompt fill:#e8eaf6,stroke:#3f51b5,stroke-width:2px,color:#1a237e;
    classDef search fill:#e0f2f1,stroke:#00897b,stroke-width:2px,color:#004d40;
    classDef fusion fill:#fff3e0,stroke:#fb8c00,stroke-width:2px,color:#e65100;
    classDef llm fill:#f3e5f5,stroke:#8e24aa,stroke-width:2px,color:#4a148c;

    UserQuery(["Input User Query"]):::startEnd

    subgraph PARALLEL_EXPANSION ["Parallel Query Expansion & HyDE Generation"]
        direction TB
        QR["queryRewriting():\nCall gpt-4o-mini with JSON Schema:\n1. stepBack query\n2. rewritten query\n3. 3 subQueries"]:::prompt
        HyDE["hydeDocument():\nCall gpt-4o-mini:\nGenerate 3-5 sentence hypothetical answer passage"]:::prompt
    end

    CompileVariants["Compile Search Variants Array:\n- rewritten\n- stepBack\n- hyde\n- subQuery1, subQuery2, subQuery3"]:::fusion
    BatchEmbedVariants["embedTexts():\nBatch embed all variant strings in single OpenAI call"]:::llm

    subgraph VECTOR_SEARCH ["Parallel Vector Search in Qdrant"]
        direction TB
        S1["Search Vector 1 (rewritten)"]:::search
        S2["Search Vector 2 (stepBack)"]:::search
        S3["Search Vector 3 (hyde)"]:::search
        S4["Search Vector 4..N (subQueries)"]:::search
    end

    RRF["reciprocalRankFusion():\nCompute RRF Score for each candidate:\nRRF_Score(doc) = Σ (1 / (k + rank))\nwhere k = 60"]:::fusion
    TopKFilter["Select Top finalK Chunks (default: 5)"]:::fusion

    subgraph ANSWER_GEN ["Grounded Answer Generation"]
        BuildContext["Build Context Window:\nFormat retrieved top chunks with sources"]:::prompt
        LLMComplete["Call gpt-4o-mini:\nSystem Prompt: 'Answer using ONLY provided context'"]:::llm
    end

    FinalResult(["Return JSON Response:\n{ query, answer, sources }"]):::startEnd

    UserQuery --> QR
    UserQuery --> HyDE
    QR --> CompileVariants
    HyDE --> CompileVariants
    CompileVariants --> BatchEmbedVariants
    BatchEmbedVariants --> S1 & S2 & S3 & S4
    S1 & S2 & S3 & S4 --> RRF
    RRF --> TopKFilter
    TopKFilter --> BuildContext
    BuildContext --> LLMComplete
    LLMComplete --> FinalResult
```

---

## 5. End-to-End Sequence Diagram (Query Execution & Polling)

This sequence diagram depicts the interaction between the Client, Express API, Redis Queue, Worker Service, Qdrant, and OpenAI during a query request lifecycle.

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant ExpressAPI as Express API Server<br/>(src/index.js)
    participant RedisQueue as BullMQ / Redis<br/>(src/queue.js)
    participant Worker as Query Worker<br/>(src/worker.js)
    participant Retriever as Retriever Module<br/>(src/retriever.js)
    participant OpenAI as OpenAI API<br/>(Embeddings & Chat)
    participant Qdrant as Qdrant DB<br/>(Vector Storage)

    Client->>ExpressAPI: POST /query { "query": "..." }
    ExpressAPI->>RedisQueue: enqueueQueryJob({ query })
    RedisQueue-->>ExpressAPI: Return Job ID (e.g. "101")
    ExpressAPI-->>Client: 202 Accepted { jobId: "101", poll: "/query/101" }

    par Polling Cycle
        Client->>ExpressAPI: GET /query/101
        ExpressAPI->>RedisQueue: getJob("101").getState()
        RedisQueue-->>ExpressAPI: "active" / "waiting"
        ExpressAPI-->>Client: 200 OK { jobId: "101", status: "active" }
    and Background Processing
        RedisQueue->>Worker: Dispatch Job 101
        Worker->>Retriever: answerQuery(query) / retrieveChunks(query)
        
        opt Advanced Query Expansion & HyDE
            Retriever->>OpenAI: chat.completions (Structured Output for Query Rewriting)
            OpenAI-->>Retriever: { stepBack, rewritten, subQueries }
            Retriever->>OpenAI: chat.completions (HyDE Hypothetical Document)
            OpenAI-->>Retriever: Hypothetical passage string
        end

        Retriever->>OpenAI: embeddings.create (Batch Query Embeddings)
        OpenAI-->>Retriever: Vector Arrays (1536 dimensions)

        Retriever->>Qdrant: search(collection, { vector, limit: topK })
        Qdrant-->>Retriever: Ranked Hit Lists

        Retriever->>Retriever: reciprocalRankFusion(rankedLists)
        
        Retriever->>OpenAI: chat.completions (System Context + Query)
        OpenAI-->>Retriever: Final Answer Text

        Retriever-->>Worker: { query, answer, sources }
        Worker->>RedisQueue: Mark Job 101 Completed & Store Return Value
    end

    Client->>ExpressAPI: GET /query/101
    ExpressAPI->>RedisQueue: getJob("101").getState() & returnvalue
    RedisQueue-->>ExpressAPI: "completed", result payload
    ExpressAPI-->>Client: 200 OK { jobId: "101", status: "completed", result: { ... } }
```

---

## 6. Source File Responsibilities & Configuration Matrix

| File Path | Primary Responsibility | Key Functions / Exports | Key Dependencies |
| :--- | :--- | :--- | :--- |
| [`src/config.js`](file:///Users/priyanshu/Desktop/ChaiCode/classwork/advance-rag-pipeline/src/config.js) | Centralized system configuration & queue constants | `config`, `INDEXING_QUEUE`, `QUERY_QUEUE` | `dotenv` |
| [`src/index.js`](file:///Users/priyanshu/Desktop/ChaiCode/classwork/advance-rag-pipeline/src/index.js) | Express HTTP API Gateway & file upload handler | `POST /index`, `POST /query`, `GET /query/:id`, `GET /health` | `express`, `multer` |
| [`src/queue.js`](file:///Users/priyanshu/Desktop/ChaiCode/classwork/advance-rag-pipeline/src/queue.js) | BullMQ client connection and job dispatchers | `enqueueIndexingJob()`, `enqueueQueryJob()`, `indexingQueue`, `queryQueue` | `bullmq`, `ioredis` |
| [`src/worker.js`](file:///Users/priyanshu/Desktop/ChaiCode/classwork/advance-rag-pipeline/src/worker.js) | Background job processor for indexing and query execution | `indexingWorker`, `queryWorker` | `bullmq` |
| [`src/indexer.js`](file:///Users/priyanshu/Desktop/ChaiCode/classwork/advance-rag-pipeline/src/indexer.js) | Document parsing, text chunking, and Qdrant ingestion | `indexPdf()`, `chunkText()`, `readPdfText()` | `pdf-parse`, `node:crypto` |
| [`src/retriever.js`](file:///Users/priyanshu/Desktop/ChaiCode/classwork/advance-rag-pipeline/src/retriever.js) | Advanced query expansion, HyDE, RRF fusion, & grounded RAG response | `queryRewriting()`, `hydeDocument()`, `retrieveChunks()`, `answerQuery()` | `openai`, `qdrant` |
| [`src/qdrant.js`](file:///Users/priyanshu/Desktop/ChaiCode/classwork/advance-rag-pipeline/src/qdrant.js) | Qdrant client connection & collection lifecycle management | `qdrant`, `ensureCollection()` | `@qdrant/js-client-rest` |
| [`src/openai.js`](file:///Users/priyanshu/Desktop/ChaiCode/classwork/advance-rag-pipeline/src/openai.js) | Shared OpenAI client & vector embedding helpers | `openai`, `embedText()`, `embedTexts()` | `openai` |

---

## 7. Configuration Parameters & Hyperparameters

Below are the key hyperparameters controlling chunking, vector dimensions, retrieval depth, and Reciprocal Rank Fusion:

```javascript
// Config settings from src/config.js
{
  chunking: {
    chunkSize: 1000,     // Character size per chunk
    chunkOverlap: 200    // Overlapping characters between consecutive chunks
  },
  qdrant: {
    collection: "documents",
    distance: "Cosine",
    dimensions: 1536    // Matches text-embedding-3-small
  },
  retrieval: {
    topK: 4,             // Top candidate chunks per individual query variant
    rrfK: 60,            // Reciprocal Rank Fusion smoothing constant (k)
    finalK: 5            // Final number of chunks passed to LLM context window
  }
}
```
