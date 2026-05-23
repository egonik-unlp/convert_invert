import { createAgent, } from "langchain";
import { z } from "zod";
import * as fs from "node:fs";


enum FileFormat {
  mp3,
  ogg,
  flac,
  aiff,
}

interface SearchItem {
  track: string,
  album: string,
  artist: string
}
interface DownloadableFile {
  filename: string,
  username: string,
  size: number
}

interface Submission {
  query: SearchItem,
  track: DownloadableFile,
}
interface Response {
  track: DownloadableFile,
  score: number
}

const getSystemPrompt = () => {
  return fs.readFileSync("./systemPrompt.txt", { encoding: "utf8" });
}

const responseFormatScore = z.object({
  score: z.number().max(1).min(0),
})
type ResponseFormat = z.infer<typeof responseFormatScore>;
// const agent = createAgent({
//   model: "openai:gpt-4.1-mini",
//   tools: [],
//   systemPrompt: getSystemPrompt(),
//   responseFormat: responseFormatScore as z.ZodTypeAny,
// });

const agent = createAgent({
  model: "openai:gpt-4.1-mini",
  systemPrompt: getSystemPrompt(),
});

async function judge(request: Submission): Promise<Response> {
  const input = JSON.stringify(request);
  console.log(`input to judge ${JSON.stringify(request)}`)
  const result = await agent.invoke({
    messages: [
      { role: "user", content: "In the next message from me you will find your input. said input will be in a format like the one I specified previously" },
      { role: "user", content: input }
    ]
  })
  const inferrence = result.structuredResponse as ResponseFormat;
  const score = inferrence.score;
  return {
    score: score,
    track: request.track
  }
}

const storage = new Map<number, string>();

Bun.serve({
  port: 6111,
  routes:
  {
    "/": new Response("Hola viejiii"),
    "/score":
    {
      POST: async (request): Promise<Response> => {
        const requestValue: Submission = await request.json();
        const response = await judge(requestValue);
        const timeStamp = Date.now();
        storage.set(Date.now(), JSON.stringify(requestValue));
        return new Response(JSON.stringify(response));
      },
      GET: () => new Response(JSON.stringify(storage))
    },
    "/score_multiple": {
      POST: async (request: any[]): Promise<Response[]> => {
        const requestValue: Submission[] = await request.json();
        const promises = [];
        for (const value of requestValue) {
          promises.push(judge(value))
        }

        const response = Promise.all(promises);
        const timeStamp = Date.now();
        storage.set(Date.now(), JSON.stringify(requestValue));
        return new Response(JSON.stringify(response));
      },

    }
  }
})
