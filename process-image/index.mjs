import {
  S3Client,
  GetObjectCommand,
  DeleteObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import sharp from "sharp";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { UpdateCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({});
const ddb = DynamoDBDocumentClient.from(client);

const DESKTOP_MAX_WIDTH = 1500;
const DESKTOP_PANORAMA_MAX_WIDTH = 2500;
const DESKTOP_MAX_HEIGHT = 1200;
const DESKTOP_MAX_SQUARE = 900;

const s3Client = new S3Client();

const streamToBuffer = (stream) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks)));
  });

export const handler = async (event) => {
  console.log("Starting image processing with event...", JSON.stringify(event));

  const key = event.Records[0].s3.object.key;
  const atlasId = key.split("/")[0];
  const markerId = key.split("/")[1];
  const photoIdWithExtension = key.split("/")[2];

  const getObjectResponse = await s3Client.send(
    new GetObjectCommand({
      Bucket: process.env.DUMP_BUCKET_NAME,
      Key: key,
    })
  );

  console.log("Retrieved image to process:", key);

  const buffer = await streamToBuffer(getObjectResponse.Body);
  console.log("Converted image to buffer");

  const image = sharp(buffer, { failOn: "none" });
  const metadata = await image.metadata();

  let imgWidth = metadata.width;
  let imgHeight = metadata.height;

  if (metadata.orientation && [5, 6, 7, 8].includes(metadata.orientation)) {
    imgWidth = metadata.height;
    imgHeight = metadata.width;
  }

  if (!imgWidth || !imgHeight) {
    throw new Error(
      `Image dimensions are not defined. Width: ${imgWidth}, Height: ${imgHeight}`
    );
  }

  let imageType = "square";
  if (imgWidth / imgHeight > 2) {
    imageType = "panorama";
  } else if (imgWidth > imgHeight) {
    imageType = "landscape";
  } else if (imgWidth < imgHeight) {
    imageType = "portrait";
  }

  let resizeTo = {};

  switch (imageType) {
    case "landscape":
      resizeTo.width =
        imgWidth > DESKTOP_MAX_WIDTH ? DESKTOP_MAX_WIDTH : imgWidth;
      break;
    case "portrait":
      resizeTo.width =
        imgHeight > DESKTOP_MAX_HEIGHT ? DESKTOP_MAX_HEIGHT : imgWidth;
      break;
    case "panorama":
      resizeTo.width = DESKTOP_PANORAMA_MAX_WIDTH;
      break;
    case "square":
      resizeTo.width = DESKTOP_MAX_SQUARE;
      resizeTo.height = DESKTOP_MAX_SQUARE;
      break;
  }

  const processedImage = await image
    .resize(resizeTo)
    .withMetadata()
    .jpeg({ quality: 60 })
    .toColorspace("srgb")
    .toBuffer();

  await s3Client.send(
    new PutObjectCommand({
      Body: processedImage,
      Bucket: process.env.OPTIMIZED_BUCKET_NAME,
      Key: key,
    })
  );

  //Delete from dump bucket
  const deleteObjectCommand = new DeleteObjectCommand({
    Bucket: process.env.DUMP_BUCKET_NAME,
    Key: key,
  });

  try {
    await s3Client.send(deleteObjectCommand);
  } catch (error) {
    console.log(`Failed to delete ${key} from dump bucket. Error: ${JSON.stringify(error)}`)
  }

  const newPhoto = {
    id: photoIdWithExtension,
    legend: ""
  };

  try {
    const command = new UpdateCommand({
      TableName: "cloud-atlas-demo-photos",
      Key: { atlasId, markerId },
      UpdateExpression: "SET photos = list_append(if_not_exists(photos, :empty), :newPhoto)",
      ExpressionAttributeValues: {
        ":newPhoto": [newPhoto],
        ":empty": []
      },
      ReturnValues: "UPDATED_NEW"
    });
  
    const result = await ddb.send(command);
    
  } catch (error) {
    console.log(`Failed to add new photo to DDB. Key:  ${key}. Error: ${JSON.stringify(error)}`)
  }

  return {
    body: { key },
    statusCode: 200,
  };
};
