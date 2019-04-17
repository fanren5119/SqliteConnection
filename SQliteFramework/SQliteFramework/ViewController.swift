//
//  ViewController.swift
//  SQliteFramework
//
//  Created by wanglei on 2019/4/17.
//  Copyright © 2019 wanglei. All rights reserved.
//

import UIKit

class ViewController: UIViewController {

    override func viewDidLoad() {
        super.viewDidLoad()
        // Do any additional setup after loading the view.
        
        //第一节课：数据库链接(打开数据库)->传统方式创建表
        let path = Bundle.main.path(forResource: "test", ofType: ".db")
        print(path!)
        let db = try! Connection(path!)
        try! db.execute("create table t_teacher(name text, email text)")
    }


}

