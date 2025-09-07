import {TraceNode} from "./trace";

export const toJSON = (root: TraceNode) => JSON.stringify(root, null, 2);

export const toMermaid = (root: TraceNode): string => {
    const lines: string[] = ["flowchart TD"];
    const seen = new Set<string>();
    const walk = (n: TraceNode) => {
        if (!seen.has(n.id)) {
            lines.push(`  ${n.id}["${n.label.replace(/"/g, '\\"')}"]`);
            seen.add(n.id);
        }
        for (const c of n.children) {
            if (!seen.has(c.id)) {
                lines.push(`  ${c.id}["${c.label.replace(/"/g, '\\"')}"]`);
                seen.add(c.id);
            }
            lines.push(`  ${n.id} --> ${c.id}`);
            walk(c);
        }
    };
    walk(root);
    return lines.join("\n");
};
