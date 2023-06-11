package edu.hanyang.submit;

import java.util.ArrayList;
import java.io.*;
import java.util.List;
import java.util.Comparator;
import java.util.PriorityQueue;
//import java.util.LinkedList;

import io.github.hyerica_bdml.indexer.ExternalSort;
import org.apache.commons.lang3.tuple.MutableTriple;
//import org.apache.lucene.util.PriorityQueue;

public class HanyangSEExternalSort implements ExternalSort {
	private int nblocks;//내부구현 및 다른 접근 방지로 private
    private int BLOCKSIZE = 1024*8;
    private int N_BLOCKS = 500;
    private int TOTALSIZE = 4000000;
	
    @Override
    
    public void sort(String infile,String outfile,String tmpdir,int blocksize,int nblocks)throws IOException{//입력파일,출력파일,임시파일,블록사이즈,블록개수를 매개변수로 하는 sort메소드 선언
    	this.nblocks=nblocks;//매개변수 nblock(블록개수)을 전달된 값으로 초기화
    	// 초기 실행을 위한 임시파일 생성
    	File temp=new File(tmpdir+File.separator +"0");//데이터 정렬하는 동안 생성된 중간 파일을 저장할 "0"하위 디렉토리 갖는 임시파일 생성,버퍼역할,하위 디렉토리 만든 이유는 유연성 및 체계성 그리고 편리한 정리, File.separator는 운영체제 간의 충돌 방지
        if (!temp.exists()){//임시파일이 존재x하면
        	temp.mkdir();//임시파일 생성
        }
    	//1)initial phase
            try(DataInputStream dis=new DataInputStream(new BufferedInputStream(new FileInputStream(infile), (blocksize * nblocks)/12))){//DataInputStream객체 생성 (데이터 읽기 위한)
                int runSize=blocksize;//실행할 블록의 크기
                int runCount=0;//실행 횟수
                ArrayList<MutableTriple<Integer,Integer,Integer>>dataArr=new ArrayList<>();//runsize크기만큼 읽음 입력 파일이 끝날 때까지 트리플 정수를 읽고 dataArr에 추가, 메모리 사용을 줄이기 위해 루프문 밖에서 선언
                String runFileBase = tmpdir + File.separator + "0" + File.separator;//기본 경로
                BufferedOutputStream bos = new BufferedOutputStream(null); //재사용을 위한 버퍼 루프 밖에서 선언
                while (dis.available()>0){//입력파일에 데이터가 남아있는 동안
//                	dataArr.clear();//재사용을 위한 clear
                    dataArr=new ArrayList<>();
                    for (int i=0;i<runSize&& dis.available()>0;i++) {//블록크기보다 작고 데이터가 남아있는 동안
                        int left=dis.readInt();//왼쪽,단어번호 읽기
                        int middle=dis.readInt();//중간 문서번호 읽기
                        int right=dis.readInt();//오른쪽 위치번호 읽기
//                        System.out.println("Triple: (" + left + ", " + middle + ", " + right + ")");//확인용 코드
                        dataArr.add(new MutableTriple<>(left, middle, right));//dataArr에 읽어들인 트리플의 왼쪽 중간 오른쪽 정수를 추가
                    }

                    dataArr.sort(Comparator.comparingInt(MutableTriple<Integer,Integer,Integer>::getLeft).thenComparingInt(MutableTriple<Integer, Integer, Integer>::getMiddle)
                    	    .thenComparingInt(MutableTriple<Integer, Integer, Integer>::getRight));//단어번호 기준으로  dataArr정렬 후 문서번호 기준 정렬 후 위치 번호 기준으로 정렬
                   
                    String runFile = runFileBase + "runcount" + runCount + ".data";//파일경로 저장하기 위해 String형태로 생성하며 임시 디렉토리의 하위 디텍토리 경로 설정
                    bos = new BufferedOutputStream(new FileOutputStream(runFile), (blocksize * nblocks)/12);//버퍼 재사용 및 새로운 FOS 할당
                    try (DataOutputStream dos = new DataOutputStream(bos)) {
						//try (DataOutputStream dos=new DataOutputStream(new BufferedOutputStream(new FileOutputStream(runFile)))) {//데이터 출력하기 위한 DataOutputStream객체 생성
						for (MutableTriple<Integer,Integer,Integer> triple : dataArr) {//dataArr에 있는 데이터를 차례대로 dos에 씀
						        dos.writeInt(triple.getLeft());//트리플 왼쪽, 단어id 쓰기
						        dos.writeInt(triple.getMiddle());//트리플 중간 문서번호 쓰기
						        dos.writeInt(triple.getRight());//트리플 오른쪽 위치번호 쓰기
						}
						//dos.flush();//dos 버퍼의 데이터 즉시 스트림
					}
                    //bos.flush();//bos 버퍼 데이터 즉시 스트림
                    
                    runCount++;//실행횟수 1씩 추가
                
            }
            bos.close();//bos 메모리 종료
    	//2)n-eway merge
    	_externalMergeSort(tmpdir,outfile,0);//외부합병정렬메소드 호출, 임시디렉토리와 출력파일,초기번호 0 전달
            }
    }
    
    private void _externalMergeSort(String tmpDir,String outputFile,int step)throws IOException{//임시파일,출력파일,단계 및 실행 횟수를 매개변수로 받는 외부합병정렬 메소드 선언
    	File[] fileArr=(new File(tmpDir+File.separator+String.valueOf(step))).listFiles();//step단계(실행횟수)에 디렉토리에 있는 파일들을 리스트화함
    	if (fileArr==null||fileArr.length==0) {//파일이 비었거나 길이가 0이면
            return;//바로 리턴
        }

        if (fileArr.length<=this.nblocks-1){//파일의 수(길이)가 블록수-1 보다 작거나 같으면 합병
            List<DataInputStream> disList=new ArrayList<>();//datainputstream 리스트 생성
            for (File f:fileArr){//각 파일 단계(횟수)마다
                disList.add(new DataInputStream(new BufferedInputStream(new FileInputStream(f))));//datalist에 fileArr로부터 읽어들여온 것을 추가
            }
            n_way_merge(disList,outputFile);//data리스트와 결과파일을 매개변수로 하는 n-way합병 메소드 호출
            
            for (DataInputStream dis :disList){//모든 datalist들에 대해
                dis.close();//사용했던 datainputstream을 닫음(메모리 절약)
            }
        } 
        else {//파일의 수나 길이가 크면
            int cnt=0;//실행된 파일의 수를 0으로 초기화
            int fileNum = 0;
            List<DataInputStream> disList=new ArrayList<>();//데이터리스트를 새롭게 초기화
            
            for (File f:fileArr) {//각 fileArr에 대해 
                disList.add(new DataInputStream(new BufferedInputStream(new FileInputStream(f))));//datalist에 fileArr로부터 읽어들여온 것을 추가
                cnt++;//실행된 횟수 1 추가6
                File nextStepDir=new File(tmpDir+File.separator+(step+1));//다음 단계의 파일 생성 및 경로 설정
                if (!nextStepDir.exists()){//다음 단계의 디렉토리가 없으면
                    nextStepDir.mkdir();//새 디렉토리 생성
                }
                if (cnt==this.nblocks- 1){//블록개수-1과 실행횟수가 같으면 (원래 if문)
                	String nextOutputFile;
                    nextOutputFile = tmpDir + File.separator + (step + 1) + File.separator + "runcount" + fileNum + ".data";//다음 단계의 결과파일을 생성
                    //String nextOutputFile=tmpDir+File.separator+(step+1)+File.separator+"runcount"+fileNum+".data";//다음 단계의결과파일을 생성
                    n_way_merge(disList,nextOutputFile);//데이터리스트와 다음 결과파일을 매개변수로 받는 n-way합병 메소드 호출
                    for (DataInputStream dis:disList){//각 데이터 리스트에 대해
                        try {
                            dis.close();//사용했던 데이터 리스트들 메모리 닫음
                        } catch (IOException e){
                            e.printStackTrace();
                        }
                    }
                    disList.clear();//모든 데이터 리스트 초기화
                    cnt = 0;//실행횟수 0으로 초기화
                    fileNum++;
                }
            }

            //if (!disList.isEmpty()){//데이터 리스트가 비어있지 않으면 (넣어도 됨)
            	//String nextOutputFile=tmpDir+File.separator+(step+1)+File.separator+"runcount"+fileNum+".data";//다음 단계의결과파일을 생성
	            String nextOutputFile;
                nextOutputFile = tmpDir + File.separator + (step + 1) + File.separator + "runcount" + fileNum + ".data";//다음 단계의 결과파일을 생성
            	n_way_merge(disList,nextOutputFile);//데이터리스트와 다음 결과파일을 매개변수로 받는 n-way합병 메소드 호출
                for (DataInputStream dis:disList){//각 데이터 리스트에 대해
                    try {
                        dis.close();//사용했던 데이터 리스트들 메모리 닫음
                    } catch (IOException e){
                        e.printStackTrace();
                    }
                }
            //}
            _externalMergeSort(tmpDir,outputFile,step+1);//임시디렉과 결과파일,다음단계 (횟수 번호)를 매개변수로 받는 외부합병정렬 메소드 호출
        }
    }
    
    //많이 수정됨
    public void n_way_merge(List<DataInputStream> files,String outputFile)throws IOException{//데이터리스트와 결과파일을 매개변수로 받는 n-way합병정렬 메소드 선언
    	//PriorityQueue<DataManager> queue=new PriorityQueue<>(files.size(),Comparator.comparing(dm -> dm.tuple));//우선큐 생성 후 datamanager에 대한 비교
    	PriorityQueue<DataManager> queue = new PriorityQueue<>(files.size(),
                Comparator.comparing(DataManager::getTuple));
		
    	
    	for (DataInputStream dis:files){//각 데이터리스트의 파일들에 대해
            DataManager dm=new DataManager(dis);//datamager 생성
            if (!dm.isEOF){//파일이 끝이 아니라면
                queue.add(dm);//큐에 datamanager 추가
            }
        }
    	try (DataOutputStream dos=new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)))){//출력 파일에 대해 dataoutputstream으로 합병한 데이터 씀
	    	while(queue.size()!=0){//큐의 사이즈가 0이 아닐 때까지
	    		DataManager dm=queue.poll();//가장 작은 값의datamanager pop
	    		MutableTriple<Integer,Integer,Integer>tmp=dm.tuple;//현 튜플을 트리플에 넣음
	    		dos.writeInt(tmp.getLeft());//트리플 왼쪽 단어번호를 dos에 쓰기
	            dos.writeInt(tmp.getMiddle());//트리플 중안 문서번호를 dos에 쓰기
	            dos.writeInt(tmp.getRight());//트리플 오른쪽 위치번호를 dos에 쓰기
	            dm.readNext();
	            if (!dm.isEOF){//파일 비어있지않으면
	                queue.add(dm);//큐에 datamager추가
	            }
	    	}
    	}
    }

    public static class DataManager{
    	public boolean isEOF=false;//파일의 끝 false로 초기화
    	private DataInputStream dis=null;//dis null로 초기화
    	public MutableTriple<Integer,Integer,Integer> tuple=new MutableTriple<Integer,Integer,Integer>(0,0,0);//0,0,0 값 갖는 튜플 생성
    	public DataManager(DataInputStream dis2)throws IOException{
    	        this.dis=dis2;
    	        isEOF=!readNext();//dis의 끝에 남은 데이터 없는지 확인
    	    
    	}
   
    	private boolean readNext()throws IOException{//다음 튜플 읽기

    		try {
	    		tuple.setLeft(dis.readInt());//트리플 왼쪽 단어번호를 dos에 읽기
	    		tuple.setMiddle(dis.readInt());//트리플 왼쪽 단어번호를 dos에 읽기
	    		tuple.setRight(dis.readInt());//트리플 왼쪽 단어번호를 dos에 읽기
	    		return true;
    		}catch(EOFException e) {//파일의 끝인 경우
    	        isEOF = true;//isEOF가 true가 됨
    	        return false;//false반환
    		}
    	}
    	public MutableTriple<Integer, Integer, Integer> getTuple() {
    	    return tuple;
    	}
    	public boolean isEOF() {
    	    return isEOF;
    	}
    }
}