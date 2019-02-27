// Parses the development applications at the South Australian District Council of Lower Eyre
// Peninsula web site and places them in a database.
//
// Michael Bone
// 27th February 2019

"use strict";

import * as fs from "fs";
import * as cheerio from "cheerio";
import * as request from "request-promise-native";
import * as sqlite3 from "sqlite3";
import * as urlparser from "url";
import * as moment from "moment";
import * as pdfjs from "pdfjs-dist";
import didYouMean, * as didyoumean from "didyoumean2";

sqlite3.verbose();

const DevelopmentApplicationsUrl = "https://www.lowereyrepeninsula.sa.gov.au/page.aspx?u=265";
const CommentUrl = "mailto:mail@dclep.sa.gov.au";

declare const process: any;

// Address information.

let StreetNames = null;
let StreetSuffixes  = null;
let SuburbNames = null;

// Sets up an sqlite database.

async function initializeDatabase() {
    return new Promise((resolve, reject) => {
        let database = new sqlite3.Database("data.sqlite");
        database.serialize(() => {
            database.run("create table if not exists [data] ([council_reference] text primary key, [address] text, [description] text, [info_url] text, [comment_url] text, [date_scraped] text, [date_received] text, [legal_description] text)");
            resolve(database);
        });
    });
}

// Inserts a row in the database if the row does not already exist.

async function insertRow(database, developmentApplication) {
    return new Promise((resolve, reject) => {
        let sqlStatement = database.prepare("insert or ignore into [data] values (?, ?, ?, ?, ?, ?, ?, ?)");
        sqlStatement.run([
            developmentApplication.applicationNumber,
            developmentApplication.address,
            developmentApplication.description,
            developmentApplication.informationUrl,
            developmentApplication.commentUrl,
            developmentApplication.scrapeDate,
            developmentApplication.receivedDate,
            developmentApplication.legalDescription
        ], function(error, row) {
            if (error) {
                console.error(error);
                reject(error);
            } else {
                if (this.changes > 0)
                    console.log(`    Inserted: application \"${developmentApplication.applicationNumber}\" with address \"${developmentApplication.address}\", description \"${developmentApplication.description}\", legal description \"${developmentApplication.legalDescription}\" and received date \"${developmentApplication.receivedDate}\" into the database.`);
                else
                    console.log(`    Skipped: application \"${developmentApplication.applicationNumber}\" with address \"${developmentApplication.address}\", description \"${developmentApplication.description}\", legal description \"${developmentApplication.legalDescription}\" and received date \"${developmentApplication.receivedDate}\" because it was already present in the database.`);
                sqlStatement.finalize();  // releases any locks
                resolve(row);
            }
        });
    });
}

// A bounding rectangle.

interface Rectangle {
    x: number,
    y: number,
    width: number,
    height: number
}

// An element (consisting of text and intersecting cells) in a PDF document.

interface Element extends Rectangle {
    text: string
}

// Reads all the address information into global objects.

function readAddressInformation() {
    // Read the street names.

    StreetNames = {}
    for (let line of fs.readFileSync("streetnames.txt").toString().replace(/\r/g, "").trim().split("\n")) {
        let streetNameTokens = line.toUpperCase().split(",");
        let streetName = streetNameTokens[0].trim();
        let suburbName = streetNameTokens[1].trim();
        (StreetNames[streetName] || (StreetNames[streetName] = [])).push(suburbName);  // several suburbs may exist for the same street name
    }

    // Read the street suffixes.

    StreetSuffixes = {};
    for (let line of fs.readFileSync("streetsuffixes.txt").toString().replace(/\r/g, "").trim().split("\n")) {
        let streetSuffixTokens = line.toUpperCase().split(",");
        StreetSuffixes[streetSuffixTokens[0].trim()] = streetSuffixTokens[1].trim();
    }

    // Read the suburb names.

    SuburbNames = {};
    for (let line of fs.readFileSync("suburbnames.txt").toString().replace(/\r/g, "").trim().split("\n")) {
        let suburbTokens = line.toUpperCase().split(",");
        
        let suburbName = suburbTokens[0].trim();
        SuburbNames[suburbName] = suburbTokens[1].trim();
        if (suburbName.startsWith("MOUNT ")) {
            SuburbNames["MT " + suburbName.substring("MOUNT ".length)] = suburbTokens[1].trim();
            SuburbNames["MT." + suburbName.substring("MOUNT ".length)] = suburbTokens[1].trim();
            SuburbNames["MT. " + suburbName.substring("MOUNT ".length)] = suburbTokens[1].trim();
        }
    }
}

// Gets the percentage of horizontal overlap between two rectangles (0 means no overlap and 100
// means 100% overlap).

function getHorizontalOverlapPercentage(rectangle1: Rectangle, rectangle2: Rectangle) {
    if (rectangle1 === undefined || rectangle2 === undefined)
        return 0;

    let startX1 = rectangle1.x;
    let endX1 = rectangle1.x + rectangle1.width;

    let startX2 = rectangle2.x;
    let endX2 = rectangle2.x + rectangle2.width;

    if (startX1 >= endX2 || endX1 <= startX2 || rectangle1.width === 0 || rectangle2.width === 0)
        return 0;

    let intersectionWidth = Math.min(endX1, endX2) - Math.max(startX1, startX2);
    let unionWidth = Math.max(endX1, endX2) - Math.min(startX1, startX2);

    return (intersectionWidth * 100) / unionWidth;
}

// Formats the text as a street.

function formatStreetName(text: string) {
    if (text === undefined)
        return text;

    let tokens = text.trim().toUpperCase().split(" ");

    // Expand the street suffix (for example, this converts "ST" to "STREET").

    let token = tokens.pop();
    let streetSuffix = StreetSuffixes[token];
    tokens.push((streetSuffix === undefined) ? token : streetSuffix);

    // Extract tokens from the end of the array until a valid street name is encountered (this
    // looks for an exact match).  Note that "PRINCESS MARGARET ROSE CAVES ROAD" is the street
    // name with the most words (ie. five).  But there may be more words in the street name due
    // to errant spaces.

    for (let index = 6; index >= 2; index--)
        if (StreetNames[tokens.slice(-index).join(" ")] !== undefined)
            return tokens.join(" ");  // reconstruct the street with the leading house number (and any other prefix text)

    // Extract tokens from the end of the array until a valid street name is encountered (this
    // allows for a spelling error).

    for (let index = 6; index >= 2; index--) {
        let threshold = 7 - index;  // set the number of allowed spelling errors proportional to the number of words
        let streetNameMatch = <string>didYouMean(tokens.slice(-index).join(" "), Object.keys(StreetNames), { caseSensitive: false, returnType: didyoumean.ReturnTypeEnums.FIRST_CLOSEST_MATCH, thresholdType: didyoumean.ThresholdTypeEnums.EDIT_DISTANCE, threshold: threshold, trimSpaces: true });
        if (streetNameMatch !== null) {
            tokens.splice(-index, index);  // remove elements from the end of the array           
            return (tokens.join(" ") + " " + streetNameMatch).trim();  // reconstruct the street with any other original prefix text
        }
    }

    return text;
}

// Formats the address, ensuring that it has a valid suburb, state and post code.

function formatAddress(address: string) {
    // Allow for a few special cases (eg. road type suffixes).

    address = address.trim().replace(/ TCE NTH/g, " TERRACE NORTH").replace(/ TCE STH/g, " TERRACE SOUTH").replace(/ TCE EAST/g, " TERRACE EAST").replace(/ TCE WEST/g, " TERRACE WEST");

    // Break the address up based on commas (the main components of the address are almost always
    // separated by commas).

    let commaIndex = address.lastIndexOf(",");
    if (commaIndex < 0)
        return address;
    let streetName = address.substring(0, commaIndex);
    let suburbName = address.substring(commaIndex + 1);

    // Add the state and post code to the suburb name.

    suburbName = <string>didYouMean(suburbName, Object.keys(SuburbNames), { caseSensitive: false, returnType: didyoumean.ReturnTypeEnums.FIRST_CLOSEST_MATCH, thresholdType: didyoumean.ThresholdTypeEnums.EDIT_DISTANCE, threshold: 2, trimSpaces: true });
    if (suburbName === null)
        return address;

    // Reconstruct the full address using the formatted street name and determined suburb name.

    return formatStreetName(streetName) + ", " + SuburbNames[suburbName];
}

// Parses the text elements from a page of a PDF.

async function parseElements(page) {
    let textContent = await page.getTextContent();

    // Find all the text elements.

    let elements: Element[] = textContent.items.map(item => {
        let transform = item.transform;

        // Work around the issue https://github.com/mozilla/pdf.js/issues/8276 (heights are
        // exaggerated).  The problem seems to be that the height value is too large in some
        // PDFs.  Provide an alternative, more accurate height value by using a calculation
        // based on the transform matrix.

        let workaroundHeight = Math.sqrt(transform[2] * transform[2] + transform[3] * transform[3]);

        let x = transform[4];
        let y = transform[5];
        let width = item.width;
        let height = workaroundHeight;

        return { text: item.str, x: x, y: y, width: width, height: height };
    });

    return elements;
}

// Parses a PDF document.

async function parsePdf(url: string) {
    console.log(`Reading development applications from ${url}.`);

    let developmentApplications = [];

    // Read the PDF.

    let buffer = await request({ url: url, encoding: null, proxy: process.env.MORPH_PROXY });
    await sleep(2000 + getRandom(0, 5) * 1000);

    // Parse the PDF.  Each page has the details of multiple applications.

    let receivedDateHeadingElement: Element;
    let lotNumberHeadingElement: Element;
    let houseNumberHeadingElement: Element;
    let streetNameHeadingElement: Element;
    let planHeadingElement: Element;
    let suburbNameHeadingElement: Element;
    let descriptionHeadingElement: Element;

    let pdf = await pdfjs.getDocument({ data: buffer, disableFontFace: true, ignoreErrors: true });
    for (let pageIndex = 0; pageIndex < pdf.numPages; pageIndex++) {
        console.log(`Reading and parsing applications from page ${pageIndex + 1} of ${pdf.numPages}.`);
        let page = await pdf.getPage(pageIndex + 1);

        // Construct elements based on the text in the PDF page.

        let elements = await parseElements(page);

        // The co-ordinate system used in a PDF is typically "upside done" so invert the
        // co-ordinates (and so this makes the subsequent logic easier to understand).

        for (let element of elements)
            element.y = -(element.y + element.height);

        // Sort the text elements by approximate Y co-ordinate and then by X co-ordinate.

        let elementComparer = (a, b) => (Math.abs(a.y - b.y) < 1) ? ((a.x > b.x) ? 1 : ((a.x < b.x) ? -1 : 0)) : ((a.y > b.y) ? 1 : -1);
        elements.sort(elementComparer);

        // Find the first column of elements.  Each element in the first column should contain
        // a development application number, for example, "371/002/17" or a column heading, for
        // example, "DEV NO.".

        let leftmostElement = elements.reduce(((previous, current) => previous === undefined ? current : (current.x < previous.x ? current : previous)), undefined);
        let leftmostElements = elements.filter(element => Math.abs(element.x - leftmostElement.x) < 20);
        let yComparer = (a, b) => (a.y > b.y) ? 1 : ((a.y < b.y) ? -1 : 0);
        leftmostElements.sort(yComparer);

        // Use the first column of elements as anchor points (the bottom, left corner is the best
        // starting point as all text for a line is bottom justified relative to the development
        // application number element).

        for (let index = 0; index < leftmostElements.length; index++) {
            // Obtain all text elements for the current development application.

            let row = elements.filter(element => element.y <= leftmostElements[index].y && (index === 0 || element.y > leftmostElements[index - 1].y));
            let leftmostElement = leftmostElements[index];

            // Extract the column headings.  Note that there is typically a different set of
            // column headings half way through the document; these represent the continuation of
            // information for development applications that was too long to fit on a single line
            // earlier in the document.

            if (index === 0 && leftmostElement.text.toUpperCase().replace(/[^A-Z]/g, "") === "DEVNO") {
                receivedDateHeadingElement = row.find(element => element.text.toUpperCase().replace(/[^A-Z]/g, "") === "LODGED");
                lotNumberHeadingElement = row.find(element => element.text.toUpperCase().replace(/[^A-Z]/g, "") === "LOTNO");
                houseNumberHeadingElement = row.find(element => element.text.toUpperCase().replace(/[^A-Z]/g, "") === "STNO");
                streetNameHeadingElement = row.find(element => element.text.toUpperCase().replace(/[^A-Z]/g, "") === "STNAME");
                planHeadingElement = row.find(element => element.text.toUpperCase().replace(/[^A-Z]/g, "") === "FPDP");
                suburbNameHeadingElement = row.find(element => element.text.toUpperCase().replace(/[^A-Z]/g, "") === "SUBURBHDOF");
                descriptionHeadingElement = row.find(element => element.text.toUpperCase().replace(/[^A-Z]/g, "") === "DESCRIPTIONOFDEVELOPMENT");
                continue;
            }

            // Development application details.

            let receivedDateElements = row.filter(element => getHorizontalOverlapPercentage(receivedDateHeadingElement, element) > 0);
            let lotNumberElements = row.filter(element => getHorizontalOverlapPercentage(lotNumberHeadingElement, element) > 0);
            let houseNumberElements = row.filter(element => getHorizontalOverlapPercentage(houseNumberHeadingElement, element) > 0);
            let streetNameElements = row.filter(element => getHorizontalOverlapPercentage(streetNameHeadingElement, element) > 0);
            let planElements = row.filter(element => getHorizontalOverlapPercentage(planHeadingElement, element) > 0);
            let suburbNameElements = row.filter(element => getHorizontalOverlapPercentage(suburbNameHeadingElement, element) > 0);
            let descriptionElements = row.filter(element => getHorizontalOverlapPercentage(descriptionHeadingElement, element) > 0);

            // Get the application number.

            let applicationNumber = leftmostElements[index].text.replace(/\s/g, "").trim();

            // Get the received date.

            let receivedDate = moment.invalid();
            if (receivedDateElements !== undefined)
                receivedDate = moment(receivedDateElements.map(element => element.text).join(" ").replace(/\s\s+/g, " ").trim(), "D-MMM-YY", true);

            // Get the lot number.

            let lotNumber = "";
            if (lotNumberElements !== undefined)
                lotNumber = lotNumberElements.map(element => element.text).join(" ").replace(/\s\s+/g, " ").trim();

            // Get the house number.

            let houseNumber = "";
            if (houseNumberElements !== undefined)
                houseNumber = houseNumberElements.map(element => element.text).join(" ").replace(/\s\s+/g, " ").trim();

            // Get the street name.

            let streetName = "";
            if (streetNameElements !== undefined)
                streetName = streetNameElements.map(element => element.text).join(" ").replace(/\s\s+/g, " ").trim();

            // Get the plan (ie. the "filed plan" or "deposited plan").

            let plan = "";
            if (planElements !== undefined)
                plan = planElements.map(element => element.text).join(" ").replace(/\s\s+/g, " ").trim();

            // Get the suburb name (and sometimes the hundred name).

            let suburbName = "";
            let hundredName = "";

            if (suburbNameElements !== undefined)
                suburbName = suburbNameElements.map(element => element.text).join(" ").replace(/\s\s+/g, " ").trim();

            let suburbNameTokens = suburbName.split("/");
            if (suburbNameTokens.length === 2) {
                if (/^HD /.test(suburbNameTokens[1].trim())) {
                    hundredName = suburbNameTokens[1].trim();  // for example, "EMU FLAT/HD CLARE"
                    suburbName = suburbNameTokens[0].trim();
                } else {
                    hundredName = suburbNameTokens[0].trim();  // for example, "HD CLARE/EMU FLAT" or "WATERLOO / MARRABEL"
                    suburbName = suburbNameTokens[1].trim();
                }
            }

            hundredName = hundredName.replace(/^HD /i, "");
            suburbName = suburbName.replace(/^HD /i, "");

            let address = formatAddress((streetName !== "" && suburbName !== "") ? `${houseNumber} ${streetName}, ${suburbName}`.toUpperCase() : "");

            // Get the description.

            let description = "";
            if (descriptionElements !== undefined)
                description = descriptionElements.map(element => element.text).join(" ").replace(/\s\s+/g, " ").trim();
            if (description === "")
                description = "No Description Provided"

            // Construct the legal description.

            let legalDescriptionItems = []
            if (lotNumber !== "")
                legalDescriptionItems.push(`Lot ${lotNumber}`);
            if (plan !== "")
                legalDescriptionItems.push(`Plan ${plan}`);
            if (hundredName !== "")
                legalDescriptionItems.push(`Hundred ${hundredName}`);
            let legalDescription = legalDescriptionItems.join(", ");

            // Create an object containing all details of the development application.

            let developmentApplication = developmentApplications[applicationNumber];
            if (developmentApplication === undefined) {
                developmentApplication = {
                    applicationNumber: applicationNumber,
                    address: "",
                    description: "No Description Provided",
                    informationUrl: url,
                    commentUrl: CommentUrl,
                    scrapeDate: moment().format("YYYY-MM-DD"),
                    receivedDate: "",
                    legalDescription: ""
                };
                developmentApplications[applicationNumber] = developmentApplication;
            }

            if (receivedDateHeadingElement !== undefined)
                developmentApplication.receivedDate = receivedDate.isValid() ? receivedDate.format("YYYY-MM-DD") : "";
            if (houseNumberHeadingElement !== undefined || streetNameHeadingElement !== undefined || suburbNameHeadingElement !== undefined)
                developmentApplication.address = address;
            if (lotNumberHeadingElement !== undefined || planHeadingElement !== undefined || suburbNameHeadingElement !== undefined)
                developmentApplication.legalDescription = legalDescription;
            if (descriptionHeadingElement !== undefined && description !== "")
                developmentApplication.description = description;
        }
    }

    // Remove any development applications with invalid addresses or application numbers.

    let filteredDevelopmentApplications = [];
    let previousApplicationNumber;
    for (let developmentApplication of Object.values(developmentApplications)) {
        if (developmentApplication.applicationNumber === "") {
            console.log(`Ignoring a development application because the application number was blank.${(previousApplicationNumber === undefined) ? "" : ("  The previous application number was " + previousApplicationNumber + ".")}`);
            continue;
        } else if (developmentApplication.address === "") {
            console.log(`Ignoring development application ${developmentApplication.applicationNumber} because the address was blank (the street name or suburb name is blank).${(previousApplicationNumber === undefined) ? "" : ("  The previous application number was " + previousApplicationNumber + ".")}`);
            continue;
        }
        previousApplicationNumber = developmentApplication.applicationNumber;
        filteredDevelopmentApplications.push(developmentApplication);
    }

    return filteredDevelopmentApplications;
}

// Gets a random integer in the specified range: [minimum, maximum).

function getRandom(minimum: number, maximum: number) {
    return Math.floor(Math.random() * (Math.floor(maximum) - Math.ceil(minimum))) + Math.ceil(minimum);
}

// Pauses for the specified number of milliseconds.

function sleep(milliseconds: number) {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}

// Parses the development applications.

async function main() {
    // Ensure that the database exists.

    let database = await initializeDatabase();

    // Read all street, street suffix and suburb information.

    readAddressInformation();

    // Read the main page of development applications.

    console.log(`Retrieving page: ${DevelopmentApplicationsUrl}`);

    let body = await request({ url: DevelopmentApplicationsUrl, rejectUnauthorized: false, proxy: process.env.MORPH_PROXY });
    await sleep(2000 + getRandom(0, 5) * 1000);
    let $ = cheerio.load(body);
    
    let pdfUrls: string[] = [];
    for (let element of $("p a").get()) {
        let pdfUrl = new urlparser.URL(element.attribs.href, DevelopmentApplicationsUrl).href;
        if (pdfUrl.toLowerCase().includes("register") && pdfUrl.toLowerCase().includes(".pdf"))
            if (!pdfUrls.some(url => url === pdfUrl))  // avoid duplicates
                pdfUrls.push(pdfUrl);
    }

    // Always parse the most recent PDF file and randomly select one other PDF file to parse.

    if (pdfUrls.length === 0) {
        console.log("No PDF URLs were found on the page.");
        return;
    }

    console.log(`Found ${pdfUrls.length} PDF file(s).  Selecting two to parse.`);

    // Select the most recent PDF.  And randomly select one other PDF (avoid processing all PDFs
    // at once because this may use too much memory, resulting in morph.io terminating the current
    // process).

    let selectedPdfUrls: string[] = [];
    selectedPdfUrls.push(pdfUrls.pop());
    if (pdfUrls.length > 0)
        selectedPdfUrls.push(pdfUrls[getRandom(0, pdfUrls.length)]);
    if (getRandom(0, 2) === 0)
        selectedPdfUrls.reverse();

    for (let pdfUrl of selectedPdfUrls) {
        console.log(`Parsing document: ${pdfUrl}`);
        let developmentApplications = await parsePdf(pdfUrl);
        console.log(`Parsed ${developmentApplications.length} development ${(developmentApplications.length == 1) ? "application" : "applications"} from document: ${pdfUrl}`);
        
        // Attempt to avoid reaching 512 MB memory usage (this will otherwise result in the
        // current process being terminated by morph.io).

        if (global.gc)
            global.gc();

        console.log("Inserting development applications into the database.");
        for (let developmentApplication of developmentApplications)
            await insertRow(database, developmentApplication);
    }
}

main().then(() => console.log("Complete.")).catch(error => console.error(error));
