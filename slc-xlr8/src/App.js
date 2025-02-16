import React, { useState, useEffect } from "react";
import "./App.css";
import { getOpenAIResponse } from "./services/openaiService";
import { readExcelFile, readTextFile, generateUmlUrl } from "./helper";
import * as XLSX from "xlsx";
import * as FileSaver from "file-saver";
import "bootstrap-icons/font/bootstrap-icons.css"; // Import Bootstrap Icons
import Loader from "./Loader";

function App() {
  const [page, setPage] = useState(0);
  const [visitedPages, setVisitedPages] = useState([
    true,
    false,
    false,
    false,
    false,
    false,
  ]);

  const [isLoading, setIsLoading] = useState(false); // Loader state
  const [txtFile, setTxtFile] = useState(null);
  const [excelFile, setExcelFile] = useState(null);
  const [plantUMLCode, setPlantUMLCode] = useState(`
    @startuml
    actor User
    User -> System : Uploads Project Requirements
    System -> User : Provides Workflow Steps
    @enduml
  `);
  const [htmlContent, setHtmlContent] = useState(
    "<h1></h1><p>This is an example HTML content.</p>"
  );
  const [additionalHtmlContent, setAdditionalHtmlContent] = useState(
    "<h1></h1><p>This is additional HTML content.</p>"
  );
  const [additionalHtmlContent1, setAdditionalHtmlContent1] = useState(
    "<h1>Additional HTML Content</h1><p>This is additional HTML content.</p>"
  );
  const [assistantResponse, setAssistantResponse] = useState("");
  const [umlUrl, setUmlUrl] = useState("");
  // const [fileType, setFileType] = useState("");
  const [rfd, setRfd] = useState("");

  const nextPage = async (e) => {
    e.preventDefault();
    setAssistantResponse("");
    setIsLoading(true);
    try {
      let fileContent;
      let prompt;

    // -------------------------------------------------------------------------------------------
    // TIMEOUT TO TEST LOADER!!!
    //   await new Promise((resolve) => setTimeout(resolve, 3000));

    // // Instead of the real response, you can set a mock response here
    // const mockResponse = "This is a simulated response for testing purposes.";
    // setAssistantResponse(mockResponse);
    // -------------------------------------------------------------------------------------------

      if (page === 0 && txtFile) {
        fileContent = await readTextFile(txtFile);
        prompt = "rfp_analyzer";
      } else if (page === 1 && excelFile) {
        fileContent = await readExcelFile(excelFile);
        prompt = "data_analyzer";
      } else if (page === 2) {
        fileContent = rfd + plantUMLCode;
        prompt = "epics";
      } else if (page === 3) {
        fileContent = htmlContent;
        prompt = "technical_design";
      } else if (page === 4) {
        fileContent = additionalHtmlContent;
        prompt = "screen_mockup";
      }
      debugger;

      const response = await getOpenAIResponse(fileContent, prompt);
      // let response = "";
      if (page === 0) {
        // response = `{ "epics": [ { "Epic": "Listing Management", "Features": [ "Product Catalog Integration: Enable seamless integration of seller's product catalog for easier listing management.", "Multi-format Listing Support: Allow listings in multiple formats including auctions and fixed-price sales for increased selling flexibility.", "Category-based Organization: Implement category-based organization of listings for improved product discoverability." ] }, { "Epic": "Inventory Management", "Features": [ "Real-time Inventory Sync: Maintain real-time inventory levels across all sales channels to prevent overselling.", "Minimum Threshold Alerts: Enable automated alerts for low stock levels to ensure timely replenishment.", "Automated Restock Triggers: Implement automated triggers for restocking items based on inventory levels and sales velocity." ] }, { "Epic": "Pricing System", "Features": [ "Dynamic Pricing Algorithms: Implement dynamic pricing algorithms to optimize listing prices based on market conditions and demand.", "Auction Management: Enable management of auction-style listings with automated bidding and closing processes.", "Bulk Pricing Updates: Allow sellers to update prices in bulk for efficient listing management." ] }, { "Epic": "Buyer Experience", "Features": [ "Advanced Search Functionality: Implement advanced search capabilities to help buyers find desired products easily.", "Secure Checkout Process: Ensure a secure checkout process with multiple payment methods for improved conversion rates.", "Order Tracking: Provide buyers with real-time tracking of their orders to enhance post-purchase experience." ] }, { "Epic": "Security", "Features": [ "Multi-factor Authentication: Implement multi-factor authentication for user accounts to enhance security.", "Encrypted Communications: Ensure all communications are encrypted to protect sensitive user data.", "Fraud Detection: Implement automated fraud detection mechanisms to protect users and maintain platform integrity." ] }, { "Epic": "Integration", "Features": [ "Third-Party System Integration: Enable integration with key third-party systems such as payment gateways, shipping carriers, and inventory management.", "RESTful API Architecture: Implement a RESTful API architecture to facilitate seamless integration and data exchange with external systems." ] }, { "Epic": "Compliance", "Features": [ "GDPR Compliance: Ensure platform complies with GDPR for data privacy and protection.", "PCI DSS Standards: Adhere to PCI DSS standards for secure payment processing and cardholder data protection." ] }, { "Epic": "Performance", "Features": [ "High Availability: Maintain 99.99% platform uptime for consistent user experience.", "Fast Load Time: Ensure page load time is under 2 seconds for improved user experience and SEO." ] } ] }`;
        setRfd(response);
      } else if (page === 1) {
        //         response = `@startuml
        // skinparam class {
        // BackgroundColor White
        // ArrowColor Black
        // BorderColor Black
        // }
        // entity "Customer" as CUST {
        // * customer_id
        // --
        // phone_number
        // email
        // status
        // }
        // entity "Restaurant" as REST {
        // * restaurant_id
        // --
        // name
        // location
        // cuisine_type
        // rating
        // status
        // }
        // entity "Order" as ORD {
        // * order_id
        // --
        // order_date
        // delivery_time
        // status
        // total_amount
        // }
        // entity "DeliveryPartner" as DEL {
        // * partner_id
        // --
        // status
        // current_location
        // rating
        // }
        // entity "MenuItem" as MENU {
        // * item_id
        // --
        // name
        // price
        // availability
        // category
        // }
        // entity "Address" as ADDR {
        // * address_id
        // --
        // location
        // type
        // }
        // entity "Payment" as PAY {
        // * payment_id
        // --
        // amount
        // status
        // payment_date
        // }
        // entity "Analytics" as ANAL {
        // * report_id
        // --
        // report_date
        // metrics
        // }
        // ' Relationships
        // CUST ||--o{ ORD
        // CUST ||--o{ ADDR
        // REST ||--o{ MENU
        // REST ||--o{ ORD
        // ORD ||--|| DEL
        // ORD ||--|| PAY
        // REST ||--o{ ANAL
        // CUST ||--o{ ANAL
        // DEL ||--o{ ANAL
        // @enduml
        // `;
        setPlantUMLCode(response);
        generateUmlUrl(response, setUmlUrl);
      } else if (page === 2) {
        setHtmlContent(response);
      } else if (page === 3) {
        setAdditionalHtmlContent(response);
      } else if (page === 4) {
        setAdditionalHtmlContent1(response);
      }
      setAssistantResponse(response);
    } catch (err) {
      console.log("Error processing the file. Please try again." + err);
    } finally {
      setIsLoading(false);
    }
    setPage((prevPage) => {
      const nextPage = prevPage + 1;
      setVisitedPages((prevVisited) => {
        const newVisited = [...prevVisited];
        newVisited[nextPage] = true;
        return newVisited;
      });
      return nextPage;
    });
  };

  const prevPage = () => setPage((prevPage) => Math.max(prevPage - 1, 0));

  return (
    <div className="app">
      {isLoading && <Loader />} {/* Display Loader when isLoading is true */}
      <ProgressBar page={page} />
      <Sidebar page={page} setPage={setPage} visitedPages={visitedPages} />
      <div className="content">
        {page === 0 && (
          <TxtUploadScreen txtFile={txtFile} setTxtFile={setTxtFile} />
        )}
        {page === 1 && (
          <DataTableScreen
            data={rfd}
            excelFile={excelFile}
            setExcelFile={setExcelFile}
          />
        )}
        {page === 2 && (
          <PlantUMLScreen
            plantUMLCode={plantUMLCode}
            setPlantUMLCode={setPlantUMLCode}
            umlUrl={umlUrl}
            setUmlUrl={setUmlUrl}
          />
        )}
        {page === 3 && (
          <HtmlContentScreen
            htmlContent={htmlContent}
            setHtmlContent={setHtmlContent}
          />
        )}
        {page === 4 && (
          <AdditionalHtmlScreen
            additionalHtmlContent={additionalHtmlContent}
            setAdditionalHtmlContent={setAdditionalHtmlContent}
          />
        )}
        {page === 5 && (
          <AdditionalHtmlScreen
            additionalHtmlContent={additionalHtmlContent1}
            setAdditionalHtmlContent={setAdditionalHtmlContent1}
          />
        )}
        {page === 6 && (
          <FinalReport
            excelFile={excelFile}
            plantUMLCode={umlUrl}
            htmlContent={htmlContent}
            additionalHtmlContent={additionalHtmlContent}
            additionalHtmlContent1={additionalHtmlContent1}
          />
        )}
      </div>

      {page > 0 && (
        <button className="arrow arrow-left" onClick={prevPage}>
          <i className="bi bi-arrow-left-circle"></i>
        </button>
      )}
      {page < 6 && (
        <button className="arrow arrow-right" onClick={nextPage}>
          <i className="bi bi-arrow-right-circle"></i>
        </button>
      )}
    </div>
  );
}

function ProgressBar({ page }) {
  const progressPercentage = ((page + 1) / 6) * 100 + 4.4;

  return (
    <div className="progress-bar-container">
      <div
        className="progress-bar"
        style={{ width: `${progressPercentage}%` }}
      />
    </div>
  );
}

function Sidebar({ page, setPage, visitedPages }) {
  return (
    <div className="sidebar">
      <h2>Workflow Progress</h2>
      {visitedPages[0] && (
        <p className={page === 0 ? "active" : ""} onClick={() => setPage(0)}>
          Project Requirements
        </p>
      )}
      {visitedPages[1] && (
        <p className={page === 1 ? "active" : ""} onClick={() => setPage(1)}>
          Epics and Features
        </p>
      )}
      {visitedPages[2] && (
        <p className={page === 2 ? "active" : ""} onClick={() => setPage(2)}>
          Database Design
        </p>
      )}
      {visitedPages[3] && (
        <p className={page === 3 ? "active" : ""} onClick={() => setPage(3)}>
          User Stories
        </p>
      )}
      {visitedPages[4] && (
        <p className={page === 4 ? "active" : ""} onClick={() => setPage(4)}>
          Technical Design Document
      
        </p>
      )}
      {visitedPages[5] && (
        <p className={page === 5 ? "active" : ""} onClick={() => setPage(5)}>
          Screen Mockups
        </p>
      )}
      {visitedPages[6] && (
        <p className={page === 6 ? "active" : ""} onClick={() => setPage(6)}>
          Project Summary - Artifacts Generated
        </p>
      )}
    </div>
  );
}

// function TxtUploadScreen({ txtFile, setTxtFile }) {
//   const handleFileChange = (e) => setTxtFile(e.target.files[0]);
//   return (
//     <div className="screen">
//       <h2>Upload Project Requirements TXT</h2>
//       <input type="file" accept=".txt" onChange={handleFileChange} />
//     </div>
//   );
// }

function TxtUploadScreen({ txtFile, setTxtFile }) {
  const handleFileChange = (e) => setTxtFile(e.target.files[0]);
  
  return (
    <div style={{
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      width: '100%',
      padding: '40px 24px',
      background: '#FFFFFF',
    }}>
      {/* Title Section */}
      <div style={{
        textAlign: 'center',
        marginBottom: '32px'
      }}>
        <h1 style={{
          fontSize: '42px',
          fontWeight: '800',
          color: '#1A1A1A',
          marginBottom: '24px',
          fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
        }}>
          SDLC Accelerate
        </h1>
        <p style={{
          fontSize: '16px',
          lineHeight: '24px',
          color: '#4B5563',
          maxWidth: '600px',
          margin: '0 auto',
          padding: '0 20px'
        }}>
          Transform your project requirements into comprehensive software architecture diagrams. 
          Our AI-powered tool analyzes your requirements and generates detailed UML diagrams, 
          streamlining your software development lifecycle.
        </p>
      </div>

      {/* Upload Section */}
      <div style={{
        background: txtFile ? '#F0FFF4' : '#F7F9FC',
        borderRadius: '16px',
        width: '100%',
        maxWidth: '480px',
        border: `1px dashed ${txtFile ? '#31C48D' : '#E4E9F0'}`,
        transition: 'all 0.2s ease',
        cursor: 'pointer',
        marginTop: '32px'
      }}>
        <input
          type="file"
          accept=".txt"
          onChange={handleFileChange}
          style={{ display: 'none' }}
          id="file-upload"
        />
        <label
          htmlFor="file-upload"
          style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            padding: '40px 24px',
            cursor: 'pointer'
          }}
        >
          {txtFile ? (
            // Uploaded State
            <>
              <div style={{
                background: '#DEF7EC',
                borderRadius: '12px',
                padding: '16px',
                marginBottom: '24px'
              }}>
                <svg width="32" height="32" viewBox="0 0 24 24" fill="none">
                  <path d="M20 6L9 17L4 12" stroke="#31C48D" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                </svg>
              </div>
              <h3 style={{
                fontSize: '16px',
                fontWeight: '600',
                color: '#1A1A1A',
                marginBottom: '8px'
              }}>
                File Uploaded Successfully
              </h3>
              <p style={{
                fontSize: '14px',
                color: '#31C48D',
                marginBottom: '4px'
              }}>
                {txtFile.name}
              </p>
              <span style={{
                fontSize: '12px',
                color: '#9CA3AF'
              }}>
                Click to upload a different file
              </span>
            </>
          ) : (
            // Default Upload State
            <>
              <div style={{
                background: '#EEF2FF',
                borderRadius: '12px',
                padding: '16px',
                marginBottom: '24px'
              }}>
                <svg width="32" height="32" viewBox="0 0 24 24" fill="none">
                  <path d="M12 15V3M12 3L7 8M12 3L17 8" stroke="#2D70FD" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                  <path d="M3 15V19C3 20.1046 3.89543 21 5 21H19C20.1046 21 21 20.1046 21 19V15" stroke="#2D70FD" strokeWidth="2" strokeLinecap="round"/>
                </svg>
              </div>
              <h3 style={{
                fontSize: '16px',
                fontWeight: '600',
                color: '#1A1A1A',
                marginBottom: '8px'
              }}>
                Upload Requirements File
              </h3>
              <p style={{
                fontSize: '14px',
                color: '#6B7280',
                marginBottom: '4px'
              }}>
                Drag and drop your file here or click to browse
              </p>
              <span style={{
                fontSize: '12px',
                color: '#9CA3AF'
              }}>
                Supported format: .txt
              </span>
            </>
          )}
        </label>
      </div>
    </div>
  );
}

function HtmlContentScreen({ htmlContent, setHtmlContent }) {
  const handleFileChange = (e) => setHtmlContent(e.target.files[0]);

  return (
    <div className="screen">
      <h2>User Stories</h2>
      <div
        className="html-content"
        dangerouslySetInnerHTML={{ __html: htmlContent }}
      />
      {/* <div className="fixed-buttons">
        <button
          className="fixed-download"
          onClick={() => downloadFile("html_content.html", htmlContent)}
        >
          Download HTML Content
        </button>
        <input type="file" accept=".html" onChange={handleFileChange} />
      </div> */}
    </div>
  );
}

function AdditionalHtmlScreen({
  additionalHtmlContent,
  setAdditionalHtmlContent,
}) {
  const handleFileChange = (e) => setAdditionalHtmlContent(e.target.files[0]);

  return (
    <div className="screen">
      <h2>Technical Design Document & Screen Mockups</h2>
      <div
        className="html-content"
        dangerouslySetInnerHTML={{ __html: additionalHtmlContent }}
      />
      {/* <div className="fixed-buttons">
        <button
          className="fixed-download"
          onClick={() =>
            downloadFile("additional_html_content.html", additionalHtmlContent)
          }
        >
          Download Additional HTML
        </button>
        <input type="file" accept=".html" onChange={handleFileChange} />
      </div> */}
    </div>
  );
}

function DataTableScreen({ data: jsonData, excelFile, setExcelFile }) {

  const [flattenedDataForTable, setFlattenedDataForTable] = useState([]);
  const [flattenedDataForExcel, setFlattenedDataForExcel] = useState([]);

  useEffect(() => {
    // Parse the JSON data if it's provided as a string
    let data;
    try {
      data = typeof jsonData === "string" ? JSON.parse(jsonData) : jsonData;
    } catch (error) {
      console.error("Invalid JSON data", error);
      return;
    }

    // Check if data.epics exists and is an array
    if (data && Array.isArray(data.epics)) {
      const tableData = [];
      const excelData = [];

      data.epics.forEach((epic) => {
        epic.Features.forEach((feature, index) => {
          if (index === 0) {
            tableData.push({
              Epic: epic.Epic,
              Feature: feature,
              RowSpan: epic.Features.length,
            });
          } else {
            tableData.push({
              Epic: "",
              Feature: feature,
              RowSpan: null,
            });
          }
          excelData.push({
            Epic: index === 0 ? epic.Epic : "",
            Feature: feature,
          });
        });
      });

      setFlattenedDataForTable(tableData);
      setFlattenedDataForExcel(excelData);
    } else {
      console.error(
        "Data is not in the expected format. Expected 'data.epics' to be an array."
      );
    }
  }, [jsonData]);

  const downloadExcelFile = () => {
    const worksheet = XLSX.utils.json_to_sheet(flattenedDataForExcel);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Sheet1");

    const excelBuffer = XLSX.write(workbook, {
      bookType: "xlsx",
      type: "array",
    });
    const blob = new Blob([excelBuffer], { type: "application/octet-stream" });
    FileSaver.saveAs(blob, "grouped_epics_data.xlsx");
  };

  return (
    <div className="screen">
      <h2>Epics and Features</h2>

      <table className="table table-bordered">
        <thead>
          <tr>
            <th>Epic</th>
            <th>Features</th>
          </tr>
        </thead>
        <tbody>
          {flattenedDataForTable.map((row, index) => (
            <tr key={index}>
              {row.Epic && <td rowSpan={row.RowSpan}>{row.Epic}</td>}
              <td>{row.Feature}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <div className="fixed-buttons">
        <button className="fixed-download" onClick={downloadExcelFile}>
          Download as Excel
        </button>
        <input
          type="file"
          accept=".xlsx"
          onChange={(e) => setExcelFile(e.target.files[0])}
        />
      </div>
    </div>
  );
}

function PlantUMLScreen({ plantUMLCode, setPlantUMLCode, umlUrl, setUmlUrl }) {
  const handleCodeChange = (e) => {
    setPlantUMLCode(e.target.value);
    generateUmlUrl(e.target.value, setUmlUrl);
  };

  return (
    <div style={{
      display: 'flex',
      width: '100%',
      height: '100%',
      minHeight: '600px'  // Add a minimum height or adjust as needed
    }}>
      <div style={{
        width: '50%',
        padding: '20px',
        borderRight: '1px solid #ccc',
        display: 'flex',
        flexDirection: 'column'
      }}>
        <h2
          style={{
            textAlign: 'center',
            marginTop: '10px',
          }}
        >Database Design</h2>
        <textarea 
          value={plantUMLCode} 
          onChange={handleCodeChange}
          style={{
            width: '90%',
            flex: 1,
            resize: 'none',
            padding: '10px',
            fontFamily: 'monospace',
            border: '1px solid #ccc',
            marginTop: '10px',
            marginLeft: '35px'
          }} 
        />
      </div>
      
      <div style={{
        width: '50%',
        padding: '20px',
        display: 'flex',
        flexDirection: 'column'
      }}>
       <h2
          style={{
            textAlign: 'center',
            marginTop: '10px',
          }}
        >
          Generated UML Diagram</h2>
        <div style={{
          flex: 1,
          overflow: 'auto',
          marginTop: '10px'
        }}>
          <img 
            src={umlUrl} 
            alt="PlantUML Diagram" 
            style={{
              width: '100%',
              height: 'auto'
            }} 
          />
        </div>
      </div>
    </div>
  );
}

// function FinalReport({
//   txtFile,
//   excelFile,
//   plantUMLCode,
//   htmlContent,
//   additionalHtmlContent,
// }) {
//   return (
//     <div className="screen">
//       <h2>Final Project Report</h2>
//       <p>Requirements TXT: {txtFile ? txtFile.name : "Not Uploaded"}</p>
//       <p>Excel Data Table: {excelFile ? excelFile.name : "Not Uploaded"}</p>
//       <pre>PlantUML Diagram Code: {plantUMLCode}</pre>
//       <div
//         className="html-preview"
//         dangerouslySetInnerHTML={{ __html: htmlContent }}
//       />
//       <div
//         className="html-preview"
//         dangerouslySetInnerHTML={{ __html: additionalHtmlContent }}
//       />
//     </div>
//   );
// }

function FinalReport({ excelFile, umlUrl, htmlContent, additionalHtmlContent, additionalHtmlContent1 }) {
  return (
    <div style={{
      height: 'calc(100vh - 100px)', // Adjust based on your header/footer
      overflow: 'auto',
      padding: '24px',
      background: '#FFFFFF'
    }}>
      <h1 style={{
        fontSize: '28px',
        fontWeight: '600',
        color: '#1A1A1A',
        marginBottom: '32px',
        textAlign: 'center'
      }}>
        Project Summary - Artifacts Generated
      </h1>

      {/* Epics and Features */}
      <div style={{
        marginBottom: '24px',
        padding: '24px',
        background: '#FFFFFF',
        borderRadius: '8px',
        border: '1px solid #E5E7EB'
      }}>
        <h2 style={{
          fontSize: '20px',
          fontWeight: '600',
          color: '#374151',
          marginBottom: '16px'
        }}>
          Epics and Features
        </h2>
        <div style={{
          maxHeight: '300px',
          overflow: 'auto',
          padding: '16px',
          background: '#F9FAFB',
          borderRadius: '6px'
        }}>
          <table style={{
            width: '100%',
            borderCollapse: 'collapse'
          }}>
            <thead>
              <tr>
                <th style={{
                  textAlign: 'left',
                  padding: '8px',
                  borderBottom: '2px solid #E5E7EB'
                }}>Epic</th>
                <th style={{
                  textAlign: 'left',
                  padding: '8px',
                  borderBottom: '2px solid #E5E7EB'
                }}>Features</th>
              </tr>
            </thead>
            <tbody>
              {Array.isArray(excelFile) && excelFile.map((row, index) => (
                <tr key={index}>
                  {row.Epic && <td style={{
                    padding: '8px',
                    borderBottom: '1px solid #E5E7EB'
                  }} rowSpan={row.RowSpan}>{row.Epic}</td>}
                  <td style={{
                    padding: '8px',
                    borderBottom: '1px solid #E5E7EB'
                  }}>{row.Feature}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Database Design */}
      <div style={{
        marginBottom: '24px',
        padding: '24px',
        background: '#FFFFFF',
        borderRadius: '8px',
        border: '1px solid #E5E7EB'
      }}>
        <h2 style={{
          fontSize: '20px',
          fontWeight: '600',
          color: '#374151',
          marginBottom: '16px'
        }}>
          Database Design
        </h2>
        <div style={{
          maxHeight: '400px',
          overflow: 'auto',
          padding: '16px',
          background: '#F9FAFB',
          borderRadius: '6px'
        }}>
          <img 
            src={umlUrl} 
            alt="Database Schema" 
            style={{
              width: '100%',
              height: 'auto'
            }}
          />
        </div>
      </div>

      {/* User Stories */}
      <div style={{
        marginBottom: '24px',
        padding: '24px',
        background: '#FFFFFF',
        borderRadius: '8px',
        border: '1px solid #E5E7EB'
      }}>
        <h2 style={{
          fontSize: '20px',
          fontWeight: '600',
          color: '#374151',
          marginBottom: '16px'
        }}>
          User Stories
        </h2>
        <div style={{
          maxHeight: '300px',
          overflow: 'auto',
          padding: '16px',
          background: '#F9FAFB',
          borderRadius: '6px'
        }}
        dangerouslySetInnerHTML={{ __html: additionalHtmlContent }} />
      </div>

      {/* Technical Design */}
      <div style={{
        marginBottom: '24px',
        padding: '24px',
        background: '#FFFFFF',
        borderRadius: '8px',
        border: '1px solid #E5E7EB'
      }}>
        <h2 style={{
          fontSize: '20px',
          fontWeight: '600',
          color: '#374151',
          marginBottom: '16px'
        }}>
          Technical Design Document
        </h2>
        <div style={{
          maxHeight: '300px',
          overflow: 'auto',
          padding: '16px',
          background: '#F9FAFB',
          borderRadius: '6px'
        }}
        dangerouslySetInnerHTML={{ __html: htmlContent }} />
      </div>

      {/* Screen Mockups */}
      <div style={{
        padding: '24px',
        background: '#FFFFFF',
        borderRadius: '8px',
        border: '1px solid #E5E7EB'
      }}>
        <h2 style={{
          fontSize: '20px',
          fontWeight: '600',
          color: '#374151',
          marginBottom: '16px'
        }}>
          Screen Mockups
        </h2>
        <div style={{
          maxHeight: '400px',
          overflow: 'auto',
          padding: '16px',
          background: '#F9FAFB',
          borderRadius: '6px'
        }}
        dangerouslySetInnerHTML={{ __html: additionalHtmlContent1 }} />
      </div>
    </div>
  );
}

function downloadFile(filename, content) {
  const blob = new Blob([content], { type: "text/html;charset=utf-8" });
  FileSaver.saveAs(blob, filename);
}

export default App;
