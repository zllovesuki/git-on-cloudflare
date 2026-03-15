import { splitHighlightedCodeIntoLines } from "@/ui/components/highlight";

type CodeViewerProps = {
  code: string;
  language?: string | null;
};

export function CodeViewer({ code, language }: CodeViewerProps) {
  const { lines, languageClass } = splitHighlightedCodeIntoLines(code, language);

  return (
    <pre id="blob-pre" className="has-line-numbers">
      <table id="blob-code" className={`code-table hljs ${languageClass}`}>
        <tbody>
          {lines.map((line, index) => {
            const lineNumber = index + 1;
            return (
              <tr key={lineNumber} id={`L${lineNumber}`} className="code-line">
                <td className="line-num">
                  <a href={`#L${lineNumber}`}>{lineNumber}</a>
                </td>
                <td
                  className="line-code"
                  dangerouslySetInnerHTML={{
                    __html: line || "&nbsp;",
                  }}
                />
              </tr>
            );
          })}
        </tbody>
      </table>
    </pre>
  );
}
