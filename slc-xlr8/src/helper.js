import * as XLSX from "xlsx";
import plantumlEncoder from "plantuml-encoder";

export const readExcelFile = (file) => {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      const data = new Uint8Array(e.target.result);
      const workbook = XLSX.read(data, { type: "array" });

      // Convert each sheet to JSON and concatenate all sheets into one JSON object
      const sheetsData = workbook.SheetNames.map((sheetName) => {
        const worksheet = workbook.Sheets[sheetName];
        return {
          sheetName,
          data: XLSX.utils.sheet_to_json(worksheet, { header: 1 }),
        };
      });

      // Convert the JSON to a text format for easier readability in GPT-4
      const formattedText = sheetsData
        .map(
          (sheet) =>
            `Sheet: ${sheet.sheetName}\n` +
            sheet.data.map((row) => row.join("\t")).join("\n")
        )
        .join("\n\n");

      resolve(formattedText);
    };
    reader.onerror = (error) => reject(error);
    reader.readAsArrayBuffer(file);
  });
};

export const readTextFile = (file) => {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (event) => resolve(event.target.result);
    reader.onerror = (error) => reject(error);
    reader.readAsText(file);
  });
};

export const generateUmlUrl = (umlCode, setUmlUrl) => {
  const encoded = plantumlEncoder.encode(umlCode);
  console.log(encoded);
  const url = `https://www.plantuml.com/plantuml/svg/${encoded}`;
  setUmlUrl(url);
};
