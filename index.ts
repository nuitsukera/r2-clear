import { S3Client, ListObjectsV2Command, DeleteObjectsCommand, ListMultipartUploadsCommand, AbortMultipartUploadCommand, _Object, ListObjectsV2CommandOutput, ListMultipartUploadsCommandOutput } from "@aws-sdk/client-s3";
import dotenv from "dotenv";

dotenv.config();

const BUCKET = process.env.R2_BUCKET!;
const ACCESS_KEY = process.env.R2_ACCESS_KEY_ID!;
const SECRET_KEY = process.env.R2_SECRET_ACCESS_KEY!;
const ACCOUNT_ID = process.env.R2_ACCOUNT_ID!;
const REGION = "auto";

const client = new S3Client({
  region: REGION,
  endpoint: `https://${ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: ACCESS_KEY,
    secretAccessKey: SECRET_KEY,
  },
});

async function listAllObjects(): Promise<string[]> {
  let objects: string[] = [];
  let continuationToken: string | undefined = undefined;

  do {
    const response: ListObjectsV2CommandOutput = await client.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      ContinuationToken: continuationToken,
    }));

    const contents: _Object[] = response.Contents || [];
    objects.push(...contents.map((obj: _Object) => obj.Key!).filter(Boolean));

    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  return objects;
}

async function abortMultipartUploads() {
  let uploads: any[] = [];
  let keyMarker: string | undefined = undefined;
  let uploadIdMarker: string | undefined = undefined;

  console.log("Checking for ongoing multipart uploads...");

  do {
    const response: ListMultipartUploadsCommandOutput = await client.send(new ListMultipartUploadsCommand({
      Bucket: BUCKET,
      KeyMarker: keyMarker,
      UploadIdMarker: uploadIdMarker,
    }));

    const currentUploads = response.Uploads || [];
    uploads.push(...currentUploads);

    keyMarker = response.NextKeyMarker;
    uploadIdMarker = response.NextUploadIdMarker;
  } while (keyMarker);

  if (uploads.length === 0) {
    console.log("No ongoing multipart uploads found.");
    return;
  }

  console.log(`Found ${uploads.length} ongoing multipart uploads. Aborting...`);

  for (const upload of uploads) {
    try {
      await client.send(new AbortMultipartUploadCommand({
        Bucket: BUCKET,
        Key: upload.Key,
        UploadId: upload.UploadId,
      }));
      console.log(`Aborted multipart upload: ${upload.Key}`);
    } catch (error) {
      console.error(`Failed to abort upload ${upload.Key}:`, error);
    }
  }

  console.log("All multipart uploads aborted!");
}

async function deleteAllObjects() {
  const objects = await listAllObjects();

  if (objects.length === 0) {
    console.log("The bucket is already empty!");
  } else {
    console.log(`Found ${objects.length} objects. Starting deletion...`);
    let deletedCount = 0;

    while (objects.length > 0) {
      const chunk = objects.splice(0, 1000);
      await client.send(new DeleteObjectsCommand({
        Bucket: BUCKET,
        Delete: {
          Objects: chunk.map((Key: string) => ({ Key })),
        },
      }));
      deletedCount += chunk.length;
      console.log(`Deleted ${chunk.length} objects (total: ${deletedCount})`);
    }

    console.log("All objects deleted!");
  }
}

async function cleanBucket() {
  console.log("Starting bucket cleanup...\n");
  
  // First, abort all ongoing multipart uploads
  await abortMultipartUploads();
  
  console.log(); // Empty line for better readability
  
  // Then, delete all objects
  await deleteAllObjects();
  
  console.log("\nBucket cleaned successfully!");
}

cleanBucket().catch(err => {
  console.error("Error cleaning bucket:", err);
  process.exit(1);
});
