#ifndef CS104_SERVER_H
#define CS104_SERVER_H

#include <napi.h>
#include <thread>
#include <atomic>
#include <mutex>
#include <vector>
#include <map>
#include <unordered_map>
#include <queue>
#include <condition_variable>

extern "C" {
#include "cs104_slave.h"
#include "hal_thread.h"
#include "hal_time.h"
}

// Структура для идентификации ожидаемой команды
struct PendingCommandKey {
    std::string clientId;
    int ioa;
    int typeId;
    int asduAddress;

    bool operator==(const PendingCommandKey& other) const {
        return clientId == other.clientId && ioa == other.ioa &&
               typeId == other.typeId && asduAddress == other.asduAddress;
    }
};

// Хеш для использования в unordered_map
namespace std {
    template<>
    struct hash<PendingCommandKey> {
        size_t operator()(const PendingCommandKey& k) const {
            return hash<string>()(k.clientId) ^ (hash<int>()(k.ioa) << 1) ^
                   (hash<int>()(k.typeId) << 2) ^ (hash<int>()(k.asduAddress) << 3);
        }
    };
}

class IEC104Server : public Napi::ObjectWrap<IEC104Server> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    IEC104Server(const Napi::CallbackInfo& info);
    virtual ~IEC104Server();
    Napi::Value SendCommandAsync(const Napi::CallbackInfo& info);

private:
    // Структура для хранения Deferred и таймера
    struct PendingCommand {
        Napi::Promise::Deferred deferred;
        std::unique_ptr<std::thread> timerThread;
        std::atomic<bool> resolved{false};
        std::mutex mtx;
        std::condition_variable cv;
        uint64_t timeoutMs;

        explicit PendingCommand(Napi::Env env) 
            : deferred(Napi::Promise::Deferred::New(env)), timeoutMs(0) {}

        // Запрещаем копирование, разрешаем перемещение
        PendingCommand(const PendingCommand&) = delete;
        PendingCommand& operator=(const PendingCommand&) = delete;
        PendingCommand(PendingCommand&&) = default;
        PendingCommand& operator=(PendingCommand&&) = default;
    };

    std::unordered_map<PendingCommandKey, std::shared_ptr<PendingCommand>> pendingCommands;
    std::mutex pendingMutex;

    // Вспомогательные методы
    void resolvePendingCommand(const PendingCommandKey& key, bool success, const std::string& errorMsg = "");
    void startTimeoutTimer(const PendingCommandKey& key, std::shared_ptr<PendingCommand> pending, uint64_t timeoutMs);

    static Napi::FunctionReference constructor;

    
    CS104_Slave server;
    std::thread _thread;
    //std::atomic<bool> running;
    std::mutex connMutex; // Synchronize connection state changes
    //bool started = false;   
    std::string ipReserve;
    std::string serverID;
    std::map<int, CS101_ASDU> asduGroups; // Пока не используется, но добавлено для будущей группировки
    int cnt = 0;   
    Napi::ThreadSafeFunction tsfn;
    bool running;
    bool started;
    //static thread_local std::string lastIpAddress;
    std::map<IMasterConnection, std::string> clientConnections; // Map of client connections to client IDs   
    std::map<std::string, int> ipConnectionCounts;
    std::map<std::string, CS104_RedundancyGroup> redundancyGroups;

    bool restrictIPs; // Флаг для ограничения IP-адресов
    CS104_ServerMode serverMode; // Режим сервера

    static bool ConnectionRequestHandler(void *parameter, const char *ipAddress);
    static void ConnectionEventHandler(void *parameter, IMasterConnection connection, CS104_PeerConnectionEvent event);
    static bool RawMessageHandler(void *parameter, IMasterConnection connection, CS101_ASDU asdu);

    Napi::Value Start(const Napi::CallbackInfo& info);
    Napi::Value Stop(const Napi::CallbackInfo& info);
    Napi::Value SendCommands(const Napi::CallbackInfo& info);
    Napi::Value GetStatus(const Napi::CallbackInfo& info);
    Napi::Value Disconnect(const Napi::CallbackInfo& info);
};

#endif // CS104_SERVER_H

