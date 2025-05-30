import { Component } from '@angular/core';

@Component({
  selector: 'app-guide',
  templateUrl: './guide.component.html',
  styleUrls: ['./guide.component.css']
})
export class GuideComponent {
  images: string[] = [
    '../../assets/step1.png',
    '../../assets/step2.png',
    '../../assets/step6.png',
    '../../assets/step3.png',
    '../../assets/step4.png',
    '../../assets/step5.png',
    '../../assets/step7.png',
    '../../assets/step8.png',
    '../../assets/step9.png',
    '../../assets/step10.png',
  ];
}