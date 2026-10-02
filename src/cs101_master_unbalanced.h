#ifndef CS101_MASTER_UNBALANCED_H
#define CS101_MASTER_UNBALANCED_H

#include <napi.h>
#include <thread>
#include <atomic>
#include <mutex>
#include <vector>
#include <map> // Добавлено для slaveStates и slaveActivated

extern "C" {
#include "hal_serial.h"
#include "cs101_master.h"
#include "hal_thread.h"
#include "hal_time.h"
}

class IEC101MasterUnbalanced : public Napi::ObjectWrap<IEC101MasterUnbalanced> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    IEC101MasterUnbalanced(const Napi::CallbackInfo& info);
    virtual ~IEC101MasterUnbalanced();
    Napi::Value RequestClass1(const Napi::CallbackInfo& info);
    Napi::Value SendInterrogation(const Napi::CallbackInfo& info);

private:   
    //std::atomic<bool> pendingClass1Poll{false};
    //std::atomic<int>  pendingClass1Slave{0};
    //uint8_t class1Fcb = 0;   // чередуется между 0x00 и 0x20

    static Napi::FunctionReference constructor;
    std::map<int, int> slaveAsduAddrMap;

    CS101_Master master;
    SerialPort serialPort;
    std::thread _thread;
    std::atomic<bool> running;
    std::mutex connMutex;
    bool connected = false;
    bool activated = false;
   
    std::string clientID;
    int cnt = 0;
    Napi::ThreadSafeFunction tsfn;
    int asduAddress = 1;
    int originatorAddress;     
    std::map<int, bool> slaveStates; // Состояние каждого слейва (true = AVAILABLE, false = ERROR/IDLE)
    std::map<int, bool> slaveActivated; // Активировано ли соединение для слейва

    static bool RawMessageHandler(void *parameter, int address, CS101_ASDU asdu);
    static void LinkLayerStateChanged(void *parameter, int address, LinkLayerState state);

    Napi::Value Connect(const Napi::CallbackInfo& info);
    Napi::Value Disconnect(const Napi::CallbackInfo& info);
    Napi::Value SendStartDT(const Napi::CallbackInfo& info);
    Napi::Value SendStopDT(const Napi::CallbackInfo& info);
    Napi::Value SendCommands(const Napi::CallbackInfo& info);
    Napi::Value GetStatus(const Napi::CallbackInfo& info);
    Napi::Value AddSlave(const Napi::CallbackInfo& info);
    Napi::Value PollSlave(const Napi::CallbackInfo& info);
};

#endif // CS101_MASTER_UNBALANCED_H