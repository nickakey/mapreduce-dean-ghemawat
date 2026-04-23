import * as fs from "fs";
import * as path from "path";

const mapBatchSize = 30; //todo make this part of class

class MasterMachine {
  #mapWorkers: Machine[] = [];
  #reduceWorkers: Machine[] = [];

  constructor(
    private mapCb: (value: string) => unknown,
    private reduceCb: (value: string) => unknown,
    private filePath: string,
  ) {}

  #workerCount = 2; // todo make this dynamic based off of something
  init() {
    // create workers
    for (let i = 0; i < this.#workerCount / 2; i++) {
      this.#mapWorkers.push(new Machine(this, "map"));
      this.#reduceWorkers.push(new Machine(this, "reduce"));
    }

    //assign map workers some work
    console.log(this.filePath);
    this.#mapWorkers.forEach((mw) => {
      mw.map(this.filePath, this.mapCb);
    });
  }
}

let iterationBlocker = 0;

class Machine {
  state: "idle" | "in-progress" | "completed" = "idle";
  constructor(
    public masterMachine: MasterMachine,
    public type: "reduce" | "map",
  ) {}
  // naming this MAP is a bit confusing
  map(filePath: string, cb: (value: string) => unknown) {
    console.log("we are mapping!!");
    const absolutePath = path.resolve(filePath);
    const fileContent = fs.readFileSync(absolutePath, "utf-8");

    //for each row in filePath
    const rows = fileContent.split("\n"); //parse from file path
    console.log({ rows });

    //batch process the rows in case there are a ton
    for (let i = 0; i < rows.length; i += mapBatchSize) {
      if (iterationBlocker > 50) {
        return;
      }
      iterationBlocker += 1;
      //1
      const mappedRows = rows.slice(i, i + mapBatchSize).map(cb);

      //2
      // Periodically, the buffered pairs are written to local disk,
      // partitioned into R regions by the partitioning function.
      // The locations of these buffered pairs on the local disk are passed back to the master,
      // who is responsible for forwarding these locations to the reduce workers.
      fs.writeFile(
        `mappedRowsBatch-${i}.json`,
        JSON.stringify(mappedRows),
        (err) => {
          if (err) {
            console.error(err);
          } else {
            console.log("file written successfully");
          }
        },
      );

      //3
      //notify the master of the locations of the rows we wrote
      console.log("DONE! Notifying!");
    }
  }
  reduce(mappedFilePath: string, cb) {
    //1
    //  When a reduce worker has read all intermediate data,
    //  it sorts it by the intermediate keys so that
    //  all occurrences of the same key are grouped together.
    //  The sorting is needed because typically many different keys
    //  map to the same reduce task.
    //2
    //  If the amount of intermediate data is too large to fit in memory, an external sort is used.
    //The reduce worker iterates over the sorted intermediate data
    // and for each unique intermediate key encountered,
    // it passes the key and the corresponding set of intermediate values
    // to the user’s Reduce function.
    // The output of the Reduce function is appended to a final output file for this reduce partition.
  }
}

const main = (inputFilePath: string) => {
  //1: split the file into chunks (how many chunks?)

  const masterMachine = new MasterMachine(
    (a: any) => a,
    (a: any) => a,
    inputFilePath,
  );
  masterMachine.init();
};

main("names.txt");
