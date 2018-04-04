import { isUrl } from '../utils/utils';

const menuData = [{
  name: '概览',
  icon: 'dashboard',
  path: 'dashboard/analysis'
}, {
  name: '数据服务',
  icon: 'form',
  path: 'form',
  children: [{
    name: '数据源定义',
    path: 'basic-form',
  }, {
    name: '计算策略',
    path: 'step-form',
  }, {
    name: '输出与存储',
    authority: 'admin',
    path: 'advanced-form',
  }],
}, {
  name: '审查',
  icon: 'profile',
  path: 'profile',
  children: [{
    name: '监听器',
    path: 'basic',
  }, {
    name: '事件',
    path: 'advanced',
    authority: 'admin',
  }],
}, {
  name: '可视化',
  icon: 'table',
  path: 'list',
  children: [{
    name: '数据表格',
    path: 'table-list',
  }, {
    name: '数据图表',
    path: 'basic-list',
  }]
}, {
  name: '设置',
  icon: 'check-circle-o',
  path: 'result',
  children: [{
    name: '成功',
    path: 'success',
  }, {
    name: '失败',
    path: 'fail',
  }],
}, {
  name: '账户',
  icon: 'user',
  path: 'user',
  authority: 'guest',
  children: [{
    name: '登录',
    path: 'login',
  }, {
    name: '注册',
    path: 'register',
  }, {
    name: '注册结果',
    path: 'register-result',
  }],
}];

function formatter(data, parentPath = '/', parentAuthority) {
  return data.map((item) => {
    let { path } = item;
    if (!isUrl(path)) {
      path = parentPath + item.path;
    }
    const result = {
      ...item,
      path,
      authority: item.authority || parentAuthority,
    };
    if (item.children) {
      result.children = formatter(item.children, `${parentPath}${item.path}/`, item.authority);
    }
    return result;
  });
}

export const getMenuData = () => formatter(menuData);
