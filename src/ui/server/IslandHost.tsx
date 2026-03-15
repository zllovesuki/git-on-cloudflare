import type { ReactNode } from "react";

type IslandHostProps = {
  name: string;
  props: unknown;
  children: ReactNode;
  className?: string;
};

function serializeProps(props: unknown): string {
  return JSON.stringify(props).replace(/[<>&\u2028\u2029]/g, (match) => {
    switch (match) {
      case "<":
        return "\\u003c";
      case ">":
        return "\\u003e";
      case "&":
        return "\\u0026";
      case "\u2028":
        return "\\u2028";
      case "\u2029":
        return "\\u2029";
      default:
        return match;
    }
  });
}

export function IslandHost({ name, props, children, className }: IslandHostProps) {
  return (
    <div data-island={name} className={className}>
      <script
        type="application/json"
        data-island-props
        dangerouslySetInnerHTML={{ __html: serializeProps(props) }}
      />
      <div data-island-root>{children}</div>
    </div>
  );
}
