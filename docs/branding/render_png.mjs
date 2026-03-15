import fs from 'node:fs';
import process from 'node:process';
import { Resvg } from '@resvg/resvg-js';

const [, , inputPath, outputPath, widthArg] = process.argv;

if (!inputPath || !outputPath || !widthArg) {
  console.error('Usage: node render_png.mjs <input.svg> <output.png> <width>');
  process.exit(1);
}

const width = Number(widthArg);
if (!Number.isFinite(width) || width <= 0) {
  console.error(`Invalid width: ${widthArg}`);
  process.exit(1);
}

const svg = fs.readFileSync(inputPath);
const resvg = new Resvg(svg, {
  fitTo: {
    mode: 'width',
    value: width,
  },
});
const pngData = resvg.render().asPng();
fs.writeFileSync(outputPath, pngData);
