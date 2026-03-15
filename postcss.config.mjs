import tailwindcss from "@tailwindcss/postcss";
import cssImport from "postcss-import";

export default () => {
  return {
    plugins: [cssImport({ path: ["node_modules"] }), tailwindcss()],
  };
};
