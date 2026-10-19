import rehypeRaw from "rehype-raw";
import { chatMarkdownRehypePlugins } from "./markdownPipeline.ts";

export const CHAT_MARKDOWN_REHYPE_PLUGINS = chatMarkdownRehypePlugins(rehypeRaw);
