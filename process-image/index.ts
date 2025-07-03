import {
    GetObjectCommand,
    GetObjectCommandOutput,
    PutObjectCommand,
    S3Client,
} from "@aws-sdk/client-s3";
import { Context, Handler } from "aws-lambda";
import sharp from "sharp";

type ImageType = "panorama" | "landscape" | "portrait" | "square";

const DESKTOP_MAX_WIDTH = 1500;
const DESKTOP_PANORAMA_MAX_WIDTH = 2500;
const DESKTOP_MAX_HEIGHT = 1200;
const DESKTOP_MAX_SQUARE = 900;

const s3Client = new S3Client({
    region: process.env.REGION,
    ...(process.env.NODE_ENV === "local" && {
        endpoint: process.env.LOCAL_ENDPOINT,
    }),
});

const streamToString = (stream: any) => {
    return new Promise((resolve, reject) => {
        const chunks: any[] = [];
        stream.on("data", (chunk: any) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks)));
    });
};

export const handler: Handler = async (
    event: { key: string; atlasId: string; markerId: string; imageId: string },
    context: Context
) => {
    console.log("Starting image processing with event...", JSON.stringify(event));

    const { key, atlasId, markerId, imageId } = event;

    let getObjectResponse: GetObjectCommandOutput;

    getObjectResponse = await s3Client.send(
        new GetObjectCommand({
            Bucket: process.env.DUMP_BUCKET_NAME,
            Key: key,
        })
    );

    console.log("Retrieved image to process: ", key);

    // SHARP PROCESSING

    const stream = (await streamToString(getObjectResponse.Body)) as Buffer;

    console.log("converted image to stream...");

    let image = sharp(stream, {
        failOn: "none",
    });
    let metadata = await image.metadata();

    let imgWidth: number | undefined = metadata.width;
    let imgHeight: number | undefined = metadata.height;

    /**
     * Sharp does something weird in that portrait photos are read as landscape
     * So the width is actually the height and vice versa
     *
     * The orientation flag tells the position of the camera when the photo was taken
     *
     * The values 5,6,7,8 are all the positions where the camera is upright
     */
    if (metadata.orientation && [5, 6, 7, 8].includes(metadata.orientation)) {
        imgWidth = metadata.height;
        imgHeight = metadata.width;
    }

    let imageType: ImageType;

    if (!imgWidth || !imgHeight) {
        throw new Error(
            `Image dimensions are not defined. Width: ${imgWidth}, Height: ${imgHeight}`
        );
    }

    if (imgWidth / imgHeight > 2) {
        imageType = "panorama";
    } else if (imgWidth > imgHeight) {
        imageType = "landscape";
    } else if (imgWidth < imgHeight) {
        imageType = "portrait";
    } else {
        imageType = "square";
    }

    let resizeTo: { width?: number; height?: number } = {};

    switch (imageType) {
        case "landscape":
            if (imgWidth > DESKTOP_MAX_WIDTH) resizeTo = { width: DESKTOP_MAX_WIDTH };
            else resizeTo = { width: metadata.width };
            break;
        case "portrait":
            if (imgHeight > DESKTOP_MAX_HEIGHT)
                resizeTo = { width: DESKTOP_MAX_HEIGHT };
            else resizeTo = { height: metadata.height };
            break;
        case "panorama":
            resizeTo = { width: DESKTOP_PANORAMA_MAX_WIDTH };
            break;
        case "square":
            resizeTo = { width: DESKTOP_MAX_SQUARE, height: DESKTOP_MAX_SQUARE };
            break;

        default:
            break;
    }

    const processedImage = await image
        .resize(resizeTo)
        .withMetadata()
        .jpeg({ quality: 60 })
        .toColorspace("srgb")
        .toBuffer();

    // SAVE TO OPTIMIZED BUCKET
    const saveProcessedImageInput = {
        Body: processedImage,
        Bucket: process.env.OPTIMIZED_BUCKET_NAME,
        Key: key,
    };
    const saveProcessedImageCommand = new PutObjectCommand(
        saveProcessedImageInput
    );

    await s3Client.send(saveProcessedImageCommand);

    return {
        body: { key },
        statusCode: 200,
    };
};