<?php

namespace zr_mail\elasticsearch;

/**
 * elastic search helper
 *      elasticserch 7.x
 */
class ElasticSearch
{
    protected $server;//'http://'.C("DB_ES_HOST").':'.C("DB_ES_PORT")
    public $index;//{index}
    protected $type;//{type}
    protected $id;//{id}

    public $index_prefix;//索引前缀
    public $ch;//curl_handler

    /**
     * ElasticSearch constructor.
     */
    function __construct($host='127.0.0.1',$port=9200,$index_prefix='email')
    {
        $this->setServer($host,$port)->setPrefix($index_prefix);
        $this->type = '_doc';

        $this->ch = curl_init();
        return $this;//直接返回this就好了，返加这个是为了兼容

    }

    /**
     * @param $host
     * @param $port
     * @return $this
     */
    public function setServer($host,$port)
    {
        $this->server = 'http://' . $host . ':' .$port;
        return $this;
    }

    /**
     * 设置前缀
     * @param string $prefix
     */
    public function setIndexPrefix($prefix)
    {
        $this->index_prefix=$prefix;
        return $this;
    }

    /**
     * 设置index prefix, 不用加*
     * @param $prefix
     * @return $this
     */
    public function setPrefix($prefix){
        $this->index_prefix = $prefix;
        return $this;
    }
    /**
     * 设置 全部索引
     * @return $this
     */
    public function ALLIndices(){
        $this->index=$this->index_prefix.'*';
        return $this;
    }

    /**
     * 自动构建index索引
     * @param null $start
     * @param null $end
     *
     * @return $this
     */
    public function _index($start = null, $end = null)
    {
        //没有传入时间戳，或者传入时间不全，则返回所有索引。
        if(is_null($start) or is_null($end)){
            $start = request('st',request('T_f', request('sdate',false)));
            $end = request('et',request('T_t', request('edate',false)));
            if ($start===false or $end===false) {//T_f、T_t,sdate、edate不存在
                return $this->ALLIndices();
            }
        }
        $start = date('YmdHis',$start);
        $end = date('YmdHis',$end);

        if($start=='19700101000000' && $end == '20381231235959'){ //仅为了保持个别页面的兼容问题，查询全部索引
            return $this->ALLIndices();
        }

        /*** 验证和处理起止时间***/
        if (!is_date($start) or !is_date($end)) {//如果输入值不对，也返回所有，并做记录。
            $this->write_log('WARN: 开始或结束时间格式不正确。start:'.$start.' end:'.$end);
            return $this->ALLIndices();
        }
        $start_default='201701010000';
        $end_default=date("YmdHis");
        if($start < $start_default){
            $start=$start_default;
        }
        if($end_default>$end_default){
            $end=$end_default;
        }
        if($start>$end){
            $this->write_log('WARN: 开始或结束时间不正确。start:'.$start.' end:'.$end);
            return $this->ALLIndices();
        }


        /*** 处理一般情况***/
        $_start_Ymd = _date("Y-m-d", $start);
        $_end_Ymd = _date("Y-m-d", $end);

        $_indices=dealIndices($_start_Ymd,$_end_Ymd,$this->index_prefix,'.');
        //$this->write_log("indices: ".join(",",$_indices));
        $this->index = join(",", $_indices);
        return $this;
    }



    function __clone()
    {
    }

    /**
     * ElasticSearch destruct.
     */
    function __destruct()
    {
        curl_close($this->ch);
        unset($this->ch);
    }

    /**
     *  原生查询，需要自建查询体
     *
     * @param string|array $DSL 完整的查询体可以是php数组可以是json
     *
     * @return mixed
     * @throws Exception
     */
    function Search($DSL)
    {
        $this->$DSL = $DSL;
        if (is_array($DSL)) {
            $DSL = _json_encode($DSL);
        }
        $OR = $this->call($this->type . '/_search?ignore_unavailable', ['method' => 'POST', 'data' => $DSL]);
        return $OR;
    }


    //具体发包curl封装
   public function _curl($url, $method = 'get', $data = '')
    {
        $ch = $this->ch; //初始化CURL句柄
		curl_setopt($ch, CURLOPT_URL, $url); //设置请求的URL
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1); //设为TRUE把curl_exec()结果转化为字串，而不是直接输出
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method); //设置请求方式
        if (strtoupper($method) == 'POST') {
            curl_setopt($ch, CURLOPT_HTTPHEADER, array("Content-Type: application/json"));
        }
        //curl_setopt($ch,CURLOPT_HTTPHEADER,array("X-HTTP-Method-Override: $method"));//设置HTTP头信息
        //curl_setopt($ch,CURLOPT_POST,1);//正规的HTTP POST，设置这个选项为一个非零值。正规的POST是普通的 application/x-www-from-urlencoded 类型，多数被HTML表单使用，我们这里不是form类型数据

        curl_setopt($ch, CURLOPT_POSTFIELDS, $data);//设置提交的字符串 , 可以是字符串，也可以是php的array数组
        $rtn['contents'] = curl_exec($ch);//执行预定义的CURL ,结果返回操作数组
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);  // 获取状态码  http://php.net/manual/en/function.curl-getinfo.php
        $rtn['httpCode'] = $httpCode;
        return $rtn;
    }

    function call($path, $http = array())
    {
        //print_r($http['data']);
        if (!$this->index) die('$this->index needs a value');
        $http['data'] = isset($http['data']) ? $http['data'] : '';
        $index = $this->index;

        $rtn_json = $this->_curl($this->server . '/' . $index . '/' . $path, $http['method'], $http['data']);
        $R = json_decode($rtn_json['contents'], true);
        if(isset($R['error'])){
            env('DEBUG') and dump($R);
            throw new \Exception('ES查询出错！'.json_encode($R));
        }
        return $R;
    }

    //设定 index
    function index($index = '')
    {
        if ($index == '') return $this->index;
        $this->index = $index;
        return $this;
    }


    //设定 type
    function type($type = null)
    {
        if (null == $type) return $this->type;
        $this->type = $type;
        return $this;
    }

    //设定 id
    function id($id = '')
    {
        if ($id == '') return $this->id;
        $this->id = $id;
        $this->index = config('custom.DB_ES_INDEX_PREFIX') . _date("Y.m.d", substr($id, 0, 14));
        return $this;
    }

    function get()
    {
        $R = $this->call($this->type . '/' . $this->id, ['method' => 'GET']);
        return $R;
    }

    function getSource()
    {
        $rtn = $this->get();
        if ($rtn['found']) {
            return $rtn['_source'];
        } else {
            return false;
        }
    }

    //curl -X PUT http://server:9200/{INDEX}/
    // 创建 索引
    function create()
    {
        return $this->call(NULL, array('method' => 'PUT'));
    }

    //curl -X DELETE http://server:9200/{INDEX}/
    // 删除 索引
    function drop()
    {
        return $this->call(NULL, array('method' => 'DELETE'));
    }
    //curl -X DELETE http://server:9200/{INDEX}/{TYPE}/{id}
    // 删除 索引
    function delete()
    {
        return $this->call($this->type . '/' . $this->id.'?refresh', array('method' => 'DELETE'));
    }


    //curl -X PUT http://server:9200/{INDEX}/{TYPE}/[{ID}] -d ...
    // 添加 数据
    // 支持 连贯 操作  ES->index("index")->type("type")->id($_GET[id])->add($data);
    // 也可以单独使用
    function add($data, $id = "")
    {
        //print_r($data);
        $data = _json_encode($data);
        if ($id == '' and $this->id == '') {
            return $this->call($this->type . '/?refresh', ['method' => 'POST', 'data' => $data]);
        } elseif ($id == '' and $this->id != '') {
            return $this->call($this->type . '/' . $this->id.'/?refresh', ['method' => 'PUT', 'data' => $data]);
        }

        return $this->call($this->type . '/' . $id.'/?refresh', ['method' => 'PUT', 'data' => $data]);
    }

    function update($field, $value = null)
    {
        if (is_null($value)) {
            if (is_array($field)) {
                $DSL = json_encode($field);
            } else {
                $DSL = $field;
            }
        } else {
            $ctx_field = 'ctx._source.' . $field;
            if (!is_int($value)) {
                $value = str_replace($field, $ctx_field, $value);
            }
            $value = es_update($value);
            $DSL = [
                "script" => [
                    "inline" => $ctx_field . '=' . $value,
                    "lang" => "painless",
                ],
                "upsert" => [
                    $field => $value,
                ],
            ];
            $DSL = json_encode($DSL);
           /* print_r($this->index);
            if($_SERVER['REMOTE_ADDR']=="192.168.1.35"){
                file_put_contents("performance_test.log",$this->index."update_index,\n",FILE_APPEND);
            }
            echo $DSL;*/
        }
        return $this->call($this->type . '/' . $this->id . '/_update?refresh', ['method' => 'POST', 'data' => $DSL]);

    }

    function deleteField($field)
    {
        $field = 'ctx._source.' . $field;
        $ctx_field = substr($field, 0, strrpos($field, '.'));
        $field = substr($field, strrpos($field, '.') + 1);
        $DSL = [
            "script" => [
                "inline" => $ctx_field . '.remove("' . $field . '")',
                "lang" => "painless",
            ],
        ];
        $DSL = json_encode($DSL);

        return $this->call($this->type . '/' . $this->id . '/_update', ['method' => 'POST', 'data' => $DSL]);
    }

    function PUT($path, $DSL)
    {
        return $this->call($path, ['method' => 'PUT', 'data' => $DSL]);
    }

    function _flush()
    {
        $index = $this->index;
        $ROOT_PATH=ROOT_PATH;
      /*  $this->redis->del('DSL:*');*/
        `rm -rf {$ROOT_PATH}zip/whitelist >/dev/null 2>&1`;
        $cmd="curl -XPOST ".$this->server."/{$index}/_flush >/dev/null 2>&1";
        return `$cmd`;
    }

    public function Delete_by_query($DSL)
    {
        $this->$DSL = $DSL;
        if (is_array($DSL)) {
            $DSL = _json_encode($DSL, JSON_PRETTY_PRINT);
        }
        return $this->call($this->type . '/_delete_by_query?refresh', ['method' => 'POST', 'data' => $DSL]);
    }

    private function write_log($str){
        if(env('DEBUG')!==false){
            /*file_put_contents(DEBUG_LOGFILE,date('[Y-m-d H:i:s] ').$str.PHP_EOL,FILE_APPEND);*/
            error_log($str);
        }
        return true;
    }

    public function mSearch($DSL){
        $rtn_json = $this->_curl($this->server . '/_msearch' , 'POST',$DSL);
        $R = json_decode($rtn_json['contents'], true);
        //print_r($R);
        if (isset($R['error'])) {
            $N_R = [
                "status" => 400,
                "info" => $R['error']['type'] . ':' . $R['error']['root_cause'][0]['reason'],
                "data" => $R,
                "DSL" => $DSL
            ];

        } else {
            $N_R = [
                "status" => 200,
                "info" => '',
                "data" => $R,
                "DSL" => $DSL
            ];

            return $R;
        }
    }


}
