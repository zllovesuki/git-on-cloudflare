interface ImportMetaEnv {
  readonly DEV: boolean;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

declare module "*.css" {
  const value: string;
  export default value;
}

declare module "*.css?url" {
  const value: string;
  export default value;
}
