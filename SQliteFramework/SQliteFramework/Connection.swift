//
//  Connection.swift
//  SQliteFramework
//
//  Created by wanglei on 2019/4/17.
//  Copyright © 2019 wanglei. All rights reserved.
//

import Foundation
import SQLite3

// 第一步: 创建Connection类管理数据库
public class Connection {
    
    // 第二步: 定义location枚举,表示数据层存储位置方式
    // 三种方式:内存数据库, 临时数据库, URI(地址)方式
    enum Location {
        
        case inMemory
        case temporary
        case uri(fileName: String)
    }
    
    //第三步: 定义数据库SQL操作类
    
    enum operation {
        case insert
        case update
        case delete
        
        init(rowValue: Int32) {
            switch rowValue {
            case SQLITE_INSERT:
                self = .insert
            case SQLITE_UPDATE:
                self = .update
            case SQLITE_DELETE:
                self = .delete
            default:
                self = .insert
            }
        }
    }
    
    // 第四步: 打开数据库,通过构造方法实现
    
    fileprivate var _handler: OpaquePointer?
    init(_ location: Location = .inMemory, _ readOnly: Bool = false) throws {
        
        // 打开数据库
        //SQLITE_OPEN_READONLY:只读数据库
        //SQLITE_OPEN_CREATE:创建数据库(没有就创建)
        //SQLITE_OPEN_READWRITE:可读可写数据库
        //SQLITE_OPEN_FULLMUTEX:设置数据库链接运行队列模式->串行队列、并行队列
        //支持多线程操作
        let flags = readOnly ? SQLITE_OPEN_READONLY : SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE
        let result = sqlite3_open_v2(location.description, &_handler, flags | SQLITE_OPEN_FULLMUTEX, nil)
        
        // 第五步: 检查数据库结构
        // 使用枚举的方式,把结构转换成枚举对象
        try checkResult(reslut: result)
        
        //第六步：定义队列->并行队列
        //主队列：串行队列
        //设置队列值(缓存当前Connaction所在的队列)
        queue.setSpecific(key: queueKey, value: queueContext)
    }
    
    convenience init(_ filename: String, _ readOnly: Bool = false) throws {
        try self.init(.uri(fileName: filename), readOnly);
    }
    
    // 第五步: 检查结果
    @discardableResult fileprivate func checkResult(reslut: Int32) throws -> Int32 {
        guard let res = Result(code: reslut, connection: self) else {
            return reslut
        }
        
        // 失败了,抛出异常
        throw res
    }
    
    // 第六步: 创建数据库执行队列
    
    // 创建并行队列
    fileprivate var queue = DispatchQueue(label: "database", attributes: [])
    
    // 根据key获取值
    fileprivate var queueKey = DispatchSpecificKey<Int>()
    //当前Connection指针->将Connection类对象引用->转为Int类型指针(引用)->强制类型转换
    //根据这个指针判定当前是串行队列，还是并行队列
    fileprivate lazy var queueContext = unsafeBitCast(self, to: Int.self)
    
    func sync<T>(_ block: () throws -> T) rethrows -> T {
        // 判断是不是当前队列
        if DispatchQueue.getSpecific(key: queueKey) == queueContext {
            return try block()
        } else {
            return try queue.sync(execute: block)
        }
    }
    
    // 第七步: 执行sql语句
    func execute(_ SQL: String) throws {
        _ = try sync {
            try checkResult(reslut: sqlite3_exec(_handler, SQL, nil, nil, nil))
        }
    }
    
    // 第八步: 定义数据库的基本操作
    // 1. 关闭数据库
    deinit {
        sqlite3_close(_handler)
    }
    
    //2、数据库状态->是否是可读性数据库
    var readOnly: Bool {
        return sqlite3_db_readonly(_handler, nil) == 1
    }
    
    //    3、最后插入的一条数据所返回的行id->rowid
    var lastInsertRowId: Int {
        return Int(sqlite3_last_insert_rowid(_handler))
    }
    
    //    4、数据库受影响行数->changes
    var changes: Int {
        return Int(sqlite3_changes(_handler))
    }
    
    //    5、数据库自从打开到目前为止数据库受影响行数
    var totalChanges: Int {
        return Int(sqlite3_total_changes(_handler))
    }
    
    //    6、中断任何长时间运行的查询操作（客户端少见）
    public func interrupt(){
        sqlite3_interrupt(_handler)
    }
    
    //    7、设置服务器超时时间
    public var busyTimeout: Double = 0 {
        didSet{
            sqlite3_busy_timeout(_handler, Int32(busyTimeout * 1_000))
        }
    }
}

extension Connection.Location: CustomStringConvertible {
    var description: String {
        switch self {
        case .inMemory:
            return ":memory"
        case .temporary:
            return ""
        case .uri(let filename):
            return filename
        }
    }
}

enum Result: Error {
    
    fileprivate static let successCodes: Set = [SQLITE_DONE, SQLITE_OK, SQLITE_ROW]
    
    case error(code: Int32, msg: String)
    
    init?(code: Int32, connection: Connection) {
        guard !Result.successCodes.contains(code) else {
            return nil
        }
        let msg = String(cString: sqlite3_errmsg(connection._handler))
        self = .error(code: code, msg: msg)
    }
}
