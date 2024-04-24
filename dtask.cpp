#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <cstring>
#include <mpi.h>
#include <map>
#include <chrono>

using namespace std;
using namespace std::chrono;

const int MAX_CHARS = 20;
const int TOP_N_CONGESTED_LIGHTS = 4;

// Define a structure to hold traffic data
struct Data {
    char time[MAX_CHARS]; // Timestamp of the traffic data
    int lightId; // ID of the traffic light
    int carsCount; // Number of cars at the traffic light
};

// Comparator function for sorting traffic data based on number of cars
bool sortData(const Data& x, const Data& y) {
    return x.carsCount > y.carsCount;
}

int main(int argc, char** argv) {
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    auto start = high_resolution_clock::now(); // Start timing

    MPI_Datatype MPI_DATA_TYPE;

    // Define MPI datatype for the custom structure
    MPI_Type_contiguous(MAX_CHARS + 2, MPI_CHAR, &MPI_DATA_TYPE); // +2 to account for lightId and carsCount
    MPI_Type_commit(&MPI_DATA_TYPE);

    // Master process code
    if (rank == 0) {
        string filename = "traffic_data.txt";
        ifstream file(filename);
        if (!file) {
            cerr << "Error opening file" << endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        vector<Data> dataList;
        string line;
        getline(file, line); // Skip header line
        // Read data from file and populate the vector
        while (getline(file, line)) {
            istringstream iss(line);
            string ind, time, lightIdStr, carsCountStr;
            getline(iss, ind, ',');
            getline(iss, time, ',');
            getline(iss, lightIdStr, ',');
            getline(iss, carsCountStr, ',');
            Data data;
            strncpy(data.time, time.c_str(), MAX_CHARS - 1);
            data.time[MAX_CHARS - 1] = '\0';
            data.lightId = stoi(lightIdStr);
            data.carsCount = stoi(carsCountStr);
            dataList.push_back(data);
        }
        file.close();

        // Partition the data and send to processes
        int dataSize = dataList.size();
        int chunkSize = dataSize / size;
        int remaining = dataSize % size;
        int offset = 0;
        for (int dest = 1; dest < size; ++dest) {
            int sendSize = chunkSize + (dest <= remaining ? 1 : 0);
            MPI_Send(&sendSize, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
            MPI_Send(&dataList[offset], sendSize, MPI_DATA_TYPE, dest, 0, MPI_COMM_WORLD);
            offset += sendSize;
        }

        // Process data in master process
        string currentTimestamp;
        map<string, vector<Data>> congestionMap; // Map to store congestion data for each timestamp
        for (int i = 0; i < chunkSize + (rank <= remaining ? 1 : 0); ++i) {
            const auto& data = dataList[i];
            // Check if the timestamp has changed or 30 minutes have elapsed
            if (currentTimestamp.empty() || strcmp(data.time, currentTimestamp.c_str()) != 0) {
                if (!currentTimestamp.empty()) {
                    // Print the timestamp and the top N most congested traffic lights
                    cout << "\nTimestamp: " << currentTimestamp << endl;
                    cout << "Top " << TOP_N_CONGESTED_LIGHTS << " most congested traffic lights:\n";
                    // Sort the traffic lights based on number of cars
                    sort(congestionMap[currentTimestamp].begin(), congestionMap[currentTimestamp].end(), sortData);
                    int count = 0;
                    for (const auto& d : congestionMap[currentTimestamp]) {
                        if (count < TOP_N_CONGESTED_LIGHTS) {
                            cout << "Traffic Light " << d.lightId << ": Number of Cars: " << d.carsCount << endl;
                            ++count;
                        } else {
                            break;
                        }
                    }
                }
                congestionMap[currentTimestamp].clear(); // Clear the congestion map for the next timestamp
                currentTimestamp = data.time;
            }
            // Update the congestion map with the current data
            congestionMap[currentTimestamp].push_back(data);
        }

        // Print the last timestamp and the top N most congested traffic lights
        if (!currentTimestamp.empty()) {
            cout << "\nTimestamp: " << currentTimestamp << endl;
            cout << "Top " << TOP_N_CONGESTED_LIGHTS << " most congested traffic lights:\n";
            // Sort the traffic lights based on number of cars
            sort(congestionMap[currentTimestamp].begin(), congestionMap[currentTimestamp].end(), sortData);
            int count = 0;
            for (const auto& d : congestionMap[currentTimestamp]) {
                if (count < TOP_N_CONGESTED_LIGHTS) {
                    cout << "Traffic Light " << d.lightId << ": Number of Cars: " << d.carsCount << endl;
                    ++count;
                } else {
                    break;
                }
            }
        }
    } else {
        // Slave process code
        int recvSize;
        MPI_Recv(&recvSize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        vector<Data> receivedData(recvSize);
        MPI_Recv(&receivedData[0], recvSize, MPI_DATA_TYPE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        // Process received data in slave process
        string currentTimestamp;
        map<string, vector<Data>> congestionMap; // Map to store congestion data for each timestamp
        for (int i = 0; i < recvSize; ++i) {
            const auto& data = receivedData[i];
            // Check if the timestamp has changed or 30 minutes have elapsed
            if (currentTimestamp.empty() || strcmp(data.time, currentTimestamp.c_str()) != 0) {
                if (!currentTimestamp.empty()) {
                    // Print the timestamp and the top N most congested traffic lights
                    cout << "\nTimestamp: " << currentTimestamp << endl;
                    cout << "Top " << TOP_N_CONGESTED_LIGHTS << " most congested traffic lights:\n";
                    // Sort the traffic lights based on number of cars
                    sort(congestionMap[currentTimestamp].begin(), congestionMap[currentTimestamp].end(), sortData);
                    int count = 0;
                    for (const auto& d : congestionMap[currentTimestamp]) {
                        if (count < TOP_N_CONGESTED_LIGHTS) {
                            cout << "Traffic Light " << d.lightId << ": Number of Cars: " << d.carsCount << endl;
                            ++count;
                        } else {
                            break;
                        }
                    }
                }
                congestionMap[currentTimestamp].clear(); // Clear the congestion map for the next timestamp
                currentTimestamp = data.time;
            }
            // Update the congestion map with the current data
            congestionMap[currentTimestamp].push_back(data);
        }

        // Print the last timestamp and the top N most congested traffic lights
        if (!currentTimestamp.empty()) {
            cout << "\nTimestamp: " << currentTimestamp << endl;
            cout << "Top " << TOP_N_CONGESTED_LIGHTS << " most congested traffic lights:\n";
            // Sort the traffic lights based on number of cars
            sort(congestionMap[currentTimestamp].begin(), congestionMap[currentTimestamp].end(), sortData);
            int count = 0;
            for (const auto& d : congestionMap[currentTimestamp]) {
                if (count < TOP_N_CONGESTED_LIGHTS) {
                    cout << "Traffic Light " << d.lightId << ": Number of Cars: " << d.carsCount << endl;
                    ++count;
                } else {
                    break;
                }
            }
        }
    }

    MPI_Type_free(&MPI_DATA_TYPE);
    MPI_Finalize();
    return 0;
}
